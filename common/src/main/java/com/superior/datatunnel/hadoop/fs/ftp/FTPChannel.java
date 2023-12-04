package com.superior.datatunnel.hadoop.fs.ftp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Set;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.net.NetUtils;
import com.superior.datatunnel.hadoop.fs.common.AbstractChannel;
import com.superior.datatunnel.hadoop.fs.common.AbstractFTPFileSystem;
import com.superior.datatunnel.hadoop.fs.common.ConnectionInfo;
import com.superior.datatunnel.hadoop.fs.common.DirTree;
import com.superior.datatunnel.hadoop.fs.common.ErrorStrings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.util.TimeZone;

import org.apache.commons.net.ftp.FTPSClient;

/**
 * Communication channel which uses org.apache.commons.net.ftp package
 * to communicate with remote FTP server. Supports HTTP proxy connections.
 */
public class FTPChannel extends AbstractChannel {

    private static final Logger LOG = LoggerFactory.getLogger(FTPChannel.class);

    // Client natively communicates with remote server
    private final FTPClient client;

    private final Path workingDir;

    static int timeoutInSeconds = 30;

    static int keepAliveMultiplier = 60;

    FTPChannel(FTPClient client, ConnectionInfo info) throws IOException {
        super(info);
        this.client = client;
        workingDir = new Path(client.printWorkingDirectory());
    }

    @Override
    public boolean isAvailable() {
        return client.isAvailable();
    }

    @Override
    public boolean isConnected() {
        return client.isConnected();
    }

    @Override
    public void destroy() throws IOException {
        client.disconnect();
    }

    @Override
    public FTPClient getNative() {
        return client;
    }

    /**
     * Creates and connects communication channel to the remote server.
     */
    static AbstractChannel create(ConnectionInfo info) {
        FTPClient client;
        String proxyHost = info.getProxyHost();
        boolean isFtp = "ftp".equals(info.getURI().getScheme().toLowerCase());

        switch (info.getProxyType()) {
            case SOCKS4:
            case SOCKS5:
                LOG.error(
                        info.logWithInfo("SOCKS proxy is not support for ftp connection"));
                return null;
            case NONE:
                if (isFtp) {
                    client = new FTPPatchedClient();
                } else {
                    client = new FTPSPatchedClient();
                }
                break;
            case HTTP:
                if (isFtp) {
                    client = new FTPHTTPTimeoutClient(proxyHost, info.getProxyPort(),
                            info.getProxyUser(), info.getProxyPassword());
                } else {
                    LOG.error(
                            info.logWithInfo("HTTP proxy is not support for ftps connection"));
                    return null;
                }
                break;
            default:
                LOG.error(info.logWithInfo("Invalid proxy type specified"));
                return null;
        }

        LOG.debug("Connection initiated");
        int port = info.getFtpPort();
        String host = info.getFtpHost();
        // 30s timeout for control connection - should be enough
        // timeout means it waits in every control socket operation for this time
        // if there is no traffic before raising an exception
        client.setDefaultTimeout(timeoutInSeconds * 1000);
        try {
            client.connect(host, port);
            int reply = client.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                // Connection can't be initiated
                throw NetUtils.wrapException(host, port,
                        NetUtils.UNKNOWN_HOST, 0,
                        new ConnectException("Server response " + reply));
            }
            String user = info.getFtpUser();
            String password = info.getFtpPassword();
            if (client.login(user, password)) {
                long keepAlive = info.isUseKeepAlive() ? (keepAliveMultiplier *
                        info.getKeepAlivePeriod()) : 0;
                client.setControlKeepAliveTimeout(keepAlive);
                // 30s timeout for data connection (get/put)
                // timeout means it waits in every data socket operation for this time
                // if there is no traffic before raising an exception
                client.setDataTimeout(timeoutInSeconds * 1000);
                client.setFileType(FTP.BINARY_FILE_TYPE);
                client.enterLocalPassiveMode();
                // We need to set enable data channel encryption
                if (!isFtp) {
                    // Set protection buffer size
                    ((FTPSClient) client).execPBSZ(0);
                    // Set data channel protection to private
                    ((FTPSClient) client).execPROT("P");
                }
            } else {
                throw new IOException("Login failed on server - " + host + ", port - " +
                        port + " as user '" + user + "'");
            }
            return new FTPChannel(client, info);
        } catch (IOException e) {
            LOG.error(info.logWithInfo("Connection can't be created"), e);
        }
        return null;
    }

    @Override
    public boolean mkdir(String parentDir, String name) {
        try {
            String workDir = client.printWorkingDirectory();
            if (client.changeWorkingDirectory(parentDir)) {
                boolean res = client.makeDirectory(name);
                client.changeWorkingDirectory(workDir);
                return res;
            }
        } catch (IOException ex) {
            LOG.error(
                    getConnectionInfo().logWithInfo(
                            String.format(ErrorStrings.E_CREATE_DIR, parentDir + name)), ex);
        }
        return false;
    }

    @Override
    public String pwd() throws IOException {
        return client.printWorkingDirectory();
    }

    @Override
    public boolean rename(String src, String dst) {
        try {
            if (client.changeWorkingDirectory("/")) {
                return client.rename(src, dst);
            }
        } catch (IOException ex) {
            LOG.error(getConnectionInfo().logWithInfo(
                    String.format(ErrorStrings.E_RENAME, src, dst)), ex);
        }
        return false;
    }

    @Override
    public boolean rm(String src) {
        try {
            return client.deleteFile(src);
        } catch (IOException ex) {
            LOG.error(getConnectionInfo().logWithInfo(ErrorStrings.E_REMOVE_FILE),
                    src, ex);
        }
        return false;
    }

    @Override
    public boolean rmdir(String src) {
        try {
            Path p = new Path(src);
            if (p.isRoot()) {
                return true;
            }
            return client.removeDirectory(src);
        } catch (IOException ex) {
            LOG.error(getConnectionInfo().logWithInfo(ErrorStrings.E_REMOVE_DIR),
                    src, ex);
        }
        return false;
    }

    @Override
    public FileStatus[] listFiles(Path dir) throws IOException {
        FTPFile[] ftpFiles;
        // Get the full list of files for given directory
        // from remote server (no caching)
        ftpFiles = client.listFiles(dir.toUri().getPath());
        ArrayList<FileStatus> fileStats = new ArrayList<>();
        for (FTPFile entry : ftpFiles) {
            String fname = entry.getName();
            // skip current and parent directory, ie. "." and ".."
            if (!".".equalsIgnoreCase(fname) && !"..".equalsIgnoreCase(fname)) {
                fileStats.add(getFileStatus(entry, dir));
            }
        }
        return fileStats.toArray(new FileStatus[fileStats.size()]);
    }

    @Override
    public FileStatus getFileStatus(Path file, Set<FileStatus> dirContentList)
            throws IOException {
        Path parentPath = file.getParent();
        // Get the special treatment for root directory
        if (parentPath == null) {
            return AbstractFTPFileSystem.getRootStatus(getConnectionInfo().getURI());
        }
        // Get the full list of files of the parent directory
        // from remote server (no caching)
        FTPFile[] ftpFiles = client.listFiles(parentPath.toUri().getPath());
        if (ftpFiles != null) {
            FileStatus fileStat = null;
            for (FTPFile ftpFile : ftpFiles) {
                FileStatus item = getFileStatus(ftpFile, parentPath);
                dirContentList.add(item);
                if (ftpFile.getName().equals(file.getName())) {
                    // That's the file we are querying
                    fileStat = item;
                }
            }
            if (fileStat == null) {
                // We didn't find the file in list.
                // Unfortunatelly it doesn't mean it doesn't exists.
                // It can be hidden file or directory
                // So let's ask for children of the file in case it's a directory
                ftpFiles = client.listFiles(file.toUri().getPath());
                if (ftpFiles == null || ftpFiles.length == 0) {
                    // Ok, it seems the file doesn't really exists or
                    // we dont have rights to access it
                    // In any case return error
                    throw new FileNotFoundException(String.format(
                            ErrorStrings.E_FILE_NOTFOUND, file));
                } else if (ftpFiles.length > 1 || !ftpFiles[0].getName().equals(
                        file.getName())) {
                    // Hidden directory - we must just hope that the file doesn't
                    // have the same name as dir
                    // as we are not able distinguishe between them at this stage
                    fileStat = new FileStatus(0, true, 1, DEFAULT_BLOCK_SIZE, -1,
                            -1, getPermissions(ftpFiles[0]), ftpFiles[0].getUser(),
                            ftpFiles[0].getGroup(), file.makeQualified(
                            getConnectionInfo().getURI(), workingDir));
                } else {
                    // Very likely hidden file
                    fileStat = getFileStatus(ftpFiles[0], parentPath);
                }
                dirContentList.add(fileStat);
            }
            return fileStat;
        } else {
            // There are no files in the directory
            throw new FileNotFoundException(
                    String.format(ErrorStrings.E_FILE_NOTFOUND, file));
        }
    }

    /**
     * Convert the file information obtained as FTPFile to a {@link FileStatus}
     * object.
     */
    private FileStatus getFileStatus(FTPFile ftpFile, Path origParent) throws
            IOException {
        Path symLink = null;
        Path parentPath = origParent;
        FTPFile linkFile = ftpFile;
        while (linkFile.isSymbolicLink()) {
            // Let's found the real file which we are pointed to by symbolic link
            String link = ftpFile.getLink();
            Path linkPath = new Path(link).makeQualified(getConnectionInfo().getURI(),
                    parentPath);
            FTPFile[] files = client.listFiles(linkPath.getParent()
                    .toUri().getPath());
            boolean found = false;
            for (FTPFile file : files) {
                if (file.getName().equals(linkPath.getName())) {
                    linkFile = file;
                    symLink = linkPath.makeQualified(getConnectionInfo().getURI(),
                            parentPath);
                    parentPath = symLink.makeQualified(getConnectionInfo().getURI(),
                            parentPath).getParent();
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new IOException("Broken link: " + link);
            }
        }
        boolean isDir = linkFile.isDirectory();
        int blockReplication = 1;
        // Using default block size since there is no way in FTP channel to know of
        // block sizes on server. The assumption could be less than ideal.
        long blockSize = DEFAULT_BLOCK_SIZE;
        long modTime = linkFile.getTimestamp().getTimeInMillis();
        long accessTime = 0;
        FsPermission permission = getPermissions(linkFile);
        String user = linkFile.getUser();
        String group = linkFile.getGroup();
        long length = ftpFile.getSize();
        Path filePath = new Path(parentPath, linkFile.getName());
        FileStatus fs = new FileStatus(length, isDir, blockReplication, blockSize,
                modTime,
                accessTime, permission, user, group,
                filePath.makeQualified(getConnectionInfo().getURI(), workingDir));
        fs.setSymlink(symLink);
        return fs;
    }

    /**
     * Return file permissions. FTP returns whole line (ls -l like) containing
     * info about file which must be parsed and permission info must be
     * identified. This method is dependent on how the remote server sends the
     * file information We may need to add extra handling as we come across the
     * servers with different "styles"
     *
     * @param ftpFile remote file information
     * @return file permission
     */
    private FsPermission getPermissions(FTPFile ftpFile) {
        String listing = ftpFile.getRawListing();
        return new FsPermission(getPermission(listing.substring(1, 4)),
                getPermission(listing.substring(4, 7)),
                getPermission(listing.substring(7, 10)));
    }

    /**
     * Parse single permission (rwx) group.
     *
     * @param perm string representing permissions
     * @return permission group
     */
    private static FsAction getPermission(String perm) {
        FsAction action = FsAction.getFsAction(perm);
        if (action == null) {
            // Microsoft FTP server doesn't return permissions in the listing.
            // Lets return default permissions
            action = FsAction.READ_EXECUTE;
        }
        return action;
    }

    @Override
    public FSDataOutputStream put(Path file, DirTree dirTree,
                                  FileSystem.Statistics statistics) throws IOException {
        // Get the data stream
        OutputStream os = client.storeFileStream(file.toUri().getPath());
        if (os != null) {
            return new FSDataOutputStream(os, statistics) {
                private boolean closed = false;

                @Override
                public synchronized void close() throws IOException {
                    if (!closed) {
                        closed = true;
                        // Close the data stream
                        super.close();
                        // Ask the control connection if transfer was completed correctly
                        boolean completed = client.completePendingCommand();
                        if (!completed) {
                            disconnect(true);
                            throw new IOException("File operation failed");
                        } else {
                            // Update the cache so it includes uploaded file
                            dirTree.addNode(FTPChannel.this, file);
                            LOG.debug("Closing data connection, " +
                                    "control connection kept open");
                            disconnect(false);
                        }
                    }
                }
            };
        } else {
            throw new IOException("Output stream not available: " + file);
        }
    }

    @Override
    public FSDataInputStream get(FileStatus file,
                                 FileSystem.Statistics statistics) throws IOException {
        LOG.debug("Getting data stream for: " + file.getPath());
        // Get the data stream
        InputStream is = getDataStream(file);
        if (is != null) {
            // All extra handling is done in FTPInputStream
            return new FSDataInputStream(
                    new FTPInputStream(is, this, file, statistics));
        } else {
            LOG.error(getConnectionInfo().logWithInfo("Input stream for file: "
                    + file.toString() + " not available"));
            throw new IOException(String.format(ErrorStrings.E_CREATE_FILE,
                    file.getPath()));
        }
    }

    @Override
    public InputStream getDataStream(FileStatus file) throws IOException {
        return client.retrieveFileStream(file.getPath().toUri().getPath());
    }

    @Override
    public void setTimes(Path p, long mtime, long atime) throws IOException {
        DateFormat tm = new SimpleDateFormat("yyyyMMddHHmmss");
        tm.setTimeZone(TimeZone.getTimeZone("GMT"));
        if (!client.setModificationTime(p.toUri().getPath(),
                tm.format(new Date(mtime)))) {
            LOG.error(getConnectionInfo().logWithInfo("Time for file: "
                    + p.toString() + " can not be modified"));
            throw new FileNotFoundException(String.format(ErrorStrings.E_MODIFY_TIME,
                    p));
        }
    }
}
