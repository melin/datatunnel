package com.superior.datatunnel.hadoop.fs.ftpextended.ftp;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.util.HashSet;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import com.superior.datatunnel.hadoop.fs.ftpextended.common.Channel;
import com.superior.datatunnel.hadoop.fs.ftpextended.common.ErrorStrings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.*;

/**
 * FTP FileSystem data input stream used when retrieving a file from remote
 * server. The stream can handle reconnections, interruptions, proxy glitches
 * ets. It's necessary to realize that FTP communication runs on 2 sockets -
 * control socket used to sent FTP commands and getting server responses and
 * data socket used to restrieve and store files This class uses data socket to
 * receive file, but uses control socket in case something goes wrong
 */
class FTPInputStream extends FSInputStream {

    private static final Logger LOG = LoggerFactory.getLogger(FTPInputStream.class);

    // Data stream
    private InputStream wrappedStream;

    private final FileSystem.Statistics stats;

    private boolean closed;

    // File status of the file we are retrieving
    private final FileStatus file;

    private final int currentSoTimeout;

    private final long keepAlive;

    private final FTPFileSystem fs;

    // Comparing value for keep alive
    private long time = System.currentTimeMillis();

    /**
     * Length of the file as detected before the transfer. If we detect reading
     * behind this value or not being able to reach this value than we try to
     * refresh it
     */
    private long realLength;

    // Current position in the stream
    private long pos;

    /**
     * Necessary for detecting change of length of the file. We don't want to do
     * the refresh all the time but only in case the reading is stopped (EOF)
     * twice at the same position
     */
    private long lastPos;

    // Communication channel to the remote server
    private FTPChannel channel;

    FTPInputStream(InputStream stream, FTPChannel channel,
                   FileStatus file, FileSystem.Statistics stats) throws IOException {

        LOG.info("Starting transfer of: {} length: {}", file.getPath(),
                file.getLen());
        checkNotNull(stream, ErrorStrings.E_NULL_INPUTSTREAM);
        this.wrappedStream = stream;
        this.stats = stats;

        this.lastPos = 0;
        this.pos = 0;
        this.closed = false;
        this.channel = channel;
        this.file = file;
        this.fs = (FTPFileSystem) file.getPath().getFileSystem(
                channel.getConnectionInfo().getConf());
        realLength = file.getLen();
        keepAlive = channel.getNative().getControlKeepAliveTimeout() * 1000;
        if (keepAlive > 0) {
            LOG.debug("Keepalive sent every " + keepAlive + "ms");
        }
        currentSoTimeout = channel.getNative().getSoTimeout();
    }

    @Override
    public synchronized void seek(long position) throws IOException {
        if (closed) {
            throw new IOException(ErrorStrings.E_STREAM_CLOSED);
        }

        if (position < 0) {
            throw new EOFException();
        }
        if (position == pos) {
            // We are already at requested position
            return;
        }
        resetSeek(position);
    }

    /**
     * Restart transfer from given position.
     *
     * @param position position to restart transfer from
     * @throws IOException
     */
    private void resetSeek(long position) throws IOException {
        closeWrappedStream();
        channel = (FTPChannel) fs.connect();
        // This is how we specify that we want to continue transfer
        // from the exact position
        channel.getNative().setRestartOffset(position);
        wrappedStream = channel.getDataStream(file);
        if (wrappedStream == null) {
            LOG.error(channel.getConnectionInfo()
                    .logWithInfo("Can't get data connection"));
            throw new IOException("Data connection not available");
        }
        pos = position;
    }

    @Override
    public synchronized int available() throws IOException {
        if (closed) {
            throw new IOException(ErrorStrings.E_STREAM_CLOSED);
        }
        return wrappedStream.available();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        throw new IOException(ErrorStrings.E_SEEK_NOTSUPPORTED);
    }

    @Override
    public long getPos() throws IOException {
        return pos;
    }

    @Override
    public synchronized int read() throws IOException {
        try {
            if (closed) {
                throw new IOException(ErrorStrings.E_STREAM_CLOSED);
            }

            int byteRead = wrappedStream.read();
            if (byteRead >= 0) {
                pos++;
            }
            if (stats != null && byteRead >= 0) {
                stats.incrementBytesRead(1);
            }
            if (byteRead == 0) {
                LOG.debug("Waiting for more input:" + pos);
            }
            if (byteRead < 0) {
                LOG.debug("End of stream:" + pos);
            }
            keepAlive(byteRead);
            return handleTruncate(byteRead);
        } catch (SocketTimeoutException e) {
            LOG.warn("Socket read timeout", e);
            // Let's handle it in handleTruncate - not being able to read from socket
            // is suspicious
            return handleTruncate(-1);
        } catch (IOException e) {
            LOG.error(channel.getConnectionInfo()
                    .logWithInfo("Error while reading: "), e);
            close();
            throw e;
        }
    }

    @Override
    public synchronized int read(byte[] buf, int off, int len)
            throws IOException {
        try {
            if (closed) {
                throw new IOException(ErrorStrings.E_STREAM_CLOSED);
            }

            int result = wrappedStream.read(buf, off, len);
            if (result > 0) {
                pos += result;
            }
            if (stats != null && result > 0) {
                stats.incrementBytesRead(result);
            }
            if (result == 0) {
                LOG.debug("Waiting for more input:" + pos);
            }
            if (result < 0) {
                LOG.debug("End of stream:" + pos);
            }
            keepAlive(result);
            return handleTruncate(result);
        } catch (SocketTimeoutException e) {
            LOG.warn("Socket read timeout", e);
            // Let's handle it in handleTruncate - not being able to read from socket
            // is suspicious
            return handleTruncate(-1);
        } catch (IOException e) {
            LOG.error(channel.getConnectionInfo()
                    .logWithInfo("Error while reading: "));
            close();
            throw e;
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }

        closed = true;
        LOG.debug("Stream closed after reading : " + pos + " bytes");
        closeWrappedStream();
    }

    /**
     * The control connection doesn't have to be necessarily closed when closing
     * data connection stream. This methods detects if the transfer was
     * successfully finished and close whole channel or just data connection
     *
     * @throws IOException communication problem when disconnecting
     */
    private void closeWrappedStream() throws IOException {
        try {
            // Can be null if data retry fails
            if (wrappedStream != null) {
                wrappedStream.close();
            }
            // Check if transfer was completed correctly
            boolean completed = channel.getNative().completePendingCommand();
            if (!completed) {
                throw new IOException("File operation failed");
            } else {
                LOG.debug("Closing data connection, control connection kept open");
                channel.disconnect(false);
            }
        } catch (IOException e) {
            LOG.debug("Closing both data connection and control connection", e);
            channel.disconnect(true);
        }
    }

    /**
     * Some proxy server will drop the connection if there is not traffic on
     * control connection for some time. Let's send noop commands in predefined
     * intervals to the server to ensure it won't happen
     *
     * @param length result of the previous read operation so the method can
     *               identify that connection is not closed
     * @throws IOException communication problem
     */
    private void keepAlive(int length) throws IOException {
        if (length != -1 && !closed) {
            long now = System.currentTimeMillis();
            if (keepAlive > 0 && now - time > keepAlive) {
                try {
                    LOG.debug("Keep alive message sent");
                    // Don't wait long for noop response. We don't expect any anyway
                    channel.getNative().setSoTimeout(10);
                    channel.getNative().noop();
                } catch (SocketTimeoutException e) {
                    LOG.debug("No keep alive response - expected", e);
                } catch (IOException e) {
                    LOG.error(channel.getConnectionInfo()
                            .logWithInfo("Unexpected error when sending keep alive"), e);
                    throw e;
                } finally {
                    // Set the original timeout
                    channel.getNative().setSoTimeout(currentSoTimeout);
                }
                time = now;
            }
        }
    }

    /**
     * If data connection is closed (length == -1) there is a chance that the
     * transfer is not actually finished because connection was closed by the
     * proxy. This method tries to detect it by comparing received length with
     * expected length. If received length is smaller than expected method will
     * try to reestablished the data connection to the server and continue with
     * the transfer from the point where it was interrupted Received length bigger
     * then expected size usually means that th downloaded file was actually
     * changed meanwhile. in that case refresh the actual length on independent
     * connection and try to process it again.
     *
     * @param length result of previous read operation -1 means EOF
     * @return result of previous read operation or 0 if connection was
     * reconnected and we try to read the rest of the file after data connection
     * was interrupted
     * @throws IOException communication problem
     */
    private int handleTruncate(int length) throws IOException {
        if (pos > realLength) {
            LOG.warn("Reading behind file length. " +
                    "Does the file changed since cached?");
            // Let's refresh the status and try again
            // We dont' want any caching so use direct query on the new connection
            if (refreshLength()) {
                // Length has changed. Run the check again
                return handleTruncate(length);
            }
            LOG.error(channel.getConnectionInfo()
                    .logWithInfo("Reading behind file length"));
            return -1;
        }
        if (length == -1 && pos < realLength) {
            // If we stop twice at the same place chances are the length of file
            // has changed on the server. So do the refresh
            if (lastPos == pos && refreshLength()) {
                // Length has changed. Run the check again
                return handleTruncate(length);
            }
            LOG.warn(
                    "Restarting transfer from position: " + pos + " Expected size: " +
                            realLength);
            resetSeek(pos);
            // we are starting from pos, store the value to lastPos so we can check
            // if we won't stop on the same place again
            lastPos = pos;
            time = System.currentTimeMillis();
            return 0;
        }
        return length;
    }

    /**
     * Asks the remote server the status of the file. It doesn't use caching so it
     * should be used only when absolutely necessary when for example verifying
     * that a file has not been change during download.
     *
     * @return true if the file length has changed - is different from cached
     * value
     * @throws IOException communication problem
     */
    private boolean refreshLength() throws IOException {
        Channel newChannel = fs.connect();
        try {
            FileStatus check = newChannel.getFileStatus(file.getPath(),
                    new HashSet<>());
            long newLength = check.getLen();
            if (newLength != realLength) {
                // File length has changed,
                LOG.debug("Length has changed: " + newLength);
                realLength = newLength;
                return true;
            } else {
                return false;
            }
        } finally {
            newChannel.disconnect(false);
        }
    }
}
