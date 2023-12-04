package com.superior.datatunnel.hadoop.fs.ftpextended.sftp;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.superior.datatunnel.hadoop.fs.ftpextended.common.ErrorStrings.E_NULL_INPUTSTREAM;
import static com.superior.datatunnel.hadoop.fs.ftpextended.common.ErrorStrings.E_STREAM_CLOSED;

import static com.google.common.base.Preconditions.*;

/**
 * SFTP FileSystem input stream. We don't do any special handling for
 * disconnections, proxy glitches etc because we didn't come across any so far.
 * If the need will arise similar handling as in FTPChannel class should
 * probably be used
 */
class SFTPInputStream extends FSInputStream {

    private static final Logger LOG = LoggerFactory.getLogger(
            SFTPInputStream.class);

    // Data stream
    private InputStream wrappedStream;

    private final FileSystem.Statistics stats;

    private final SFTPFileSystem fs;

    private final FileStatus file;

    private long realLength;

    private boolean closed;

    private long pos;

    // Communication channel to the remote server
    private SFTPChannel channel;

    SFTPInputStream(InputStream stream, SFTPChannel channel,
                    FileStatus file, FileSystem.Statistics stats) throws IOException {

        checkNotNull(stream, E_NULL_INPUTSTREAM);
        this.wrappedStream = stream;
        this.stats = stats;

        this.pos = 0;
        this.closed = false;
        this.channel = channel;
        this.file = file;
        this.fs = (SFTPFileSystem) file.getPath().getFileSystem(
                channel.getConnectionInfo().getConf());
        this.realLength = file.getLen();
    }

    @Override
    public void seek(long position) throws IOException {
        this.wrappedStream.skip(position);
        this.pos = position;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

    @Override
    public long getPos() throws IOException {
        return pos;
    }

    @Override
    public synchronized int read() throws IOException {
        if (closed) {
            throw new IOException(E_STREAM_CLOSED);
        }

        int byteRead = wrappedStream.read();
        if (byteRead >= 0) {
            pos++;
        }
        if (stats != null && byteRead >= 0) {
            stats.incrementBytesRead(1);
        }
        return byteRead;
    }

    @Override
    public synchronized int read(byte[] buf, int off, int len)
            throws IOException {
        if (closed) {
            throw new IOException(E_STREAM_CLOSED);
        }

        int result = wrappedStream.read(buf, off, len);
        if (result > 0) {
            pos += result;
        }
        if (stats != null && result > 0) {
            stats.incrementBytesRead(result);
        }

        return result;
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        try {
            wrappedStream.close();
            closed = true;
            //channel.disconnect(false);
            channel.destroy();
        } catch (IOException e) {
            LOG.debug("Failed to close connection", e);
            channel.disconnect(true);
        }
    }
}
