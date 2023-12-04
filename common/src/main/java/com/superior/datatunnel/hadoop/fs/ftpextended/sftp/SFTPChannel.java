package com.superior.datatunnel.hadoop.fs.ftpextended.sftp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;
import java.util.List;

import com.superior.datatunnel.hadoop.fs.ftpextended.common.AbstractChannel;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.superior.datatunnel.hadoop.fs.ftpextended.common.AbstractFTPFileSystem;
import com.superior.datatunnel.hadoop.fs.ftpextended.common.ConnectionInfo;
import com.superior.datatunnel.hadoop.fs.ftpextended.common.DirTree;
import com.superior.datatunnel.hadoop.fs.ftpextended.common.ErrorStrings;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Proxy;
import com.jcraft.jsch.ProxyHTTP;
import com.jcraft.jsch.ProxySOCKS4;
import com.jcraft.jsch.ProxySOCKS5;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

import java.util.Date;

import org.apache.commons.lang3.StringUtils;

/**
 * Communication channel which uses link com.jcraft.jsch package to
 * communicate with remote SFTP server. Channel support HTTP and SOCKS4/5
 * proxies.
 */
public class SFTPChannel extends AbstractChannel {

    private static final Logger LOG = LoggerFactory.getLogger(SFTPChannel.class);

    // Client natively communicates with remote server
    private final ChannelSftp client;

    private final Path workingDir;

    SFTPChannel(ChannelSftp client, ConnectionInfo info) throws SftpException {
        super(info);
        this.client = client;
        workingDir = new Path(client.pwd());
    }

    @Override
    public boolean isAvailable() {
        return client.isConnected();
    }

    @Override
    public boolean isConnected() {
        return client.isConnected();
    }

    @Override
    public void destroy() throws IOException {
        try {
            client.getSession().disconnect();
        } catch (JSchException ex) {
            LOG.error(getConnectionInfo().logWithInfo("Can't close sftp session"));
            throw new IOException(ex.toString(), ex);
        }
    }

    static AbstractChannel create(ConnectionInfo info) {
        JSch jsch = new JSch();
        Session session;

        int port = info.getFtpPort();
        String host = info.getFtpHost();
        String user = info.getFtpUser();
        String password = info.getFtpPassword();
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");

        try {
            session = jsch.getSession(user, host, port);
            if (password == null || password.isEmpty()) {
                //First try to get key private key from credentials provider
                byte[] key = info.getKey();
                if (key != null) {
                    jsch.addIdentity(user, key,
                            null, info.getKeyPassPhrase());
                }
            } else {
                session.setPassword(password);
            }
            session.setConfig(config);
            session.setProxy(getProxy(info));
            session.connect();

            ChannelSftp channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect();

            return new SFTPChannel(channel, info);

        } catch (JSchException | SftpException e) {
            LOG.error(info.logWithInfo("Exception while connecting:"), e);
            return null;
        }
    }

    private static Proxy getProxy(ConnectionInfo info) {
        String proxyHost = info.getProxyHost();
        int proxyPort = info.getProxyPort();
        Proxy proxy = null;
        switch (info.getProxyType()) {
            case SOCKS4:
                ProxySOCKS4 proxy4 = new ProxySOCKS4(proxyHost, proxyPort);
                proxy4.setUserPasswd(info.getProxyUser(), info.getProxyPassword());
                proxy = proxy4;
                break;
            case SOCKS5:
                ProxySOCKS5 proxy5 = new ProxySOCKS5(proxyHost, proxyPort);
                proxy5.setUserPasswd(info.getProxyUser(), info.getProxyPassword());
                proxy = proxy5;
                break;
            case HTTP:
                ProxyHTTP proxyHttp = new ProxyHTTP(proxyHost, proxyPort);
                proxyHttp.setUserPasswd(info.getProxyUser(), info.getProxyPassword());
                proxy = proxyHttp;
                break;
            case NONE:
                break;
            default:
                LOG.error(info.logWithInfo("Invalid proxy type specified"));
        }
        return proxy;
    }

    @Override
    public ChannelSftp getNative() {
        return client;
    }

    @Override
    public FSDataOutputStream put(Path file, DirTree dirTree,
                                  FileSystem.Statistics statistics) throws IOException {
        try {
            // Get the data stream
            OutputStream os = client.put(file.toUri().getPath());
            if (os != null) {
                return new FSDataOutputStream(os, statistics) {
                    private boolean closed = false;

                    @Override
                    public synchronized void close() throws IOException {
                        if (!closed) {
                            closed = true;
                            // Close the data stream
                            super.close();
                            // Update the cache so it includes uploaded file
                            dirTree.addNode(SFTPChannel.this, file);
                            LOG.debug("Closing data connection, " +
                                    "control connection kept open");
                            disconnect(false);
                        }
                    }
                };
            } else {
                throw new IOException("Output stream not available: " + file);
            }
        } catch (SftpException ex) {
            throw new IOException(ex.toString() + " " +
                    String.format(ErrorStrings.E_CREATE_FILE, file), ex);
        }
    }

    @Override
    public boolean mkdir(String parentDir, String name) {
        try {
            client.cd(parentDir);
            client.mkdir(name);
        } catch (SftpException e) {
            LOG.error(getConnectionInfo().logWithInfo(
                    String.format(ErrorStrings.E_CREATE_DIR, parentDir + name)), e);
            return false;
        }
        return true;
    }

    @Override
    public boolean rename(String src, String dst) {
        try {
            client.cd("/");
            client.rename(src, dst);
        } catch (SftpException ex) {
            LOG.error(getConnectionInfo().logWithInfo(
                    String.format(ErrorStrings.E_RENAME, src, dst)), ex);
            return false;
        }
        return true;
    }

    @Override
    public String pwd() throws IOException {
        try {
            return client.pwd();
        } catch (SftpException ex) {
            LOG.error(getConnectionInfo().logWithInfo(ErrorStrings.E_FAILED_GETHOME),
                    ex);
            throw new IOException(ex.toString(), ex);
        }
    }

    @Override
    public boolean rm(String src) {
        try {
            client.rm(src);
            return true;
        } catch (SftpException e) {
            LOG.error(getConnectionInfo().logWithInfo(ErrorStrings.E_REMOVE_FILE),
                    src, e);
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
            client.rmdir(src);
            return true;
        } catch (SftpException e) {
            LOG.error(getConnectionInfo().logWithInfo(ErrorStrings.E_REMOVE_DIR),
                    src, e);
        }
        return false;
    }

    @Override
    public FileStatus[] listFiles(Path dir) throws IOException {
        List<ChannelSftp.LsEntry> sftpFiles;
        try {
            // Get the full list of files for given directory from remote server
            //(no caching)
            sftpFiles = client.ls(dir.toUri().getPath());
        } catch (SftpException e) {
            throw new IOException(e.toString(), e);
        }
        ArrayList<FileStatus> fileStats = new ArrayList<>();
        for (int i = 0; i < sftpFiles.size(); i++) {
            ChannelSftp.LsEntry entry = sftpFiles.get(i);
            String fname = entry.getFilename();
            // skip current and parent directory, ie. "." and ".."
            if (!".".equalsIgnoreCase(fname) && !"..".equalsIgnoreCase(fname)) {
                fileStats.add(getFileStatus(entry, dir));
            }
        }
        return fileStats.toArray(new FileStatus[fileStats.size()]);
    }

    /**
     * Convert the file information as received from remote server to a
     * {@link FileStatus} object.
     */
    private FileStatus getFileStatus(ChannelSftp.LsEntry sftpFile,
                                     Path origParent) throws IOException {

        Path parentPath = origParent;
        SftpATTRS attr = sftpFile.getAttrs();
        String link = parentPath.toUri().getPath() + "/" + sftpFile.getFilename();
        String symLink = null;
        while (attr.isLink()) {
            // Let's found the real file which we are pointed to by symbolic link
            try {
                Path qualified = new Path(client.readlink(link)).makeQualified(
                        getConnectionInfo().getURI(), parentPath);
                symLink = qualified.toUri().getPath();
                link = symLink;
                parentPath = qualified.getParent();
                attr = client.lstat(symLink);
            } catch (SftpException e) {
                throw new IOException("Broken link: " + e.toString(), e);
            }
        }
        long length = attr.getSize();
        boolean isDir = attr.isDir();
        int blockReplication = 1;
        // Using default block size since there is no way in SFTP channel to know of
        // block sizes on server. The assumption could be less than ideal.
        long blockSize = DEFAULT_BLOCK_SIZE;
        // convert to milliseconds
        long modTime = attr.getMTime() * 1000L;
        long accessTime = attr.getATime() * 1000L;
        FsPermission permission = getPermissions(sftpFile);
        // not be able to get the real user group name, just use the user and group
        // id
        String longName = sftpFile.getLongname();
        String user = getUser(longName, attr);
        String group = getGroup(longName, attr);
        Path filePath = new Path(parentPath, sftpFile.getFilename());

        FileStatus fs = new FileStatus(length, isDir, blockReplication, blockSize,
                modTime,
                accessTime, permission, user, group,
                filePath.makeQualified(getConnectionInfo().getURI(), workingDir));
        fs.setSymlink(symLink != null ? new Path(symLink).makeQualified(
                getConnectionInfo().getURI(), parentPath) : null);
        return fs;
    }

    private String getUser(String longName, SftpATTRS attr) {

        String[] elements = StringUtils.split(longName);
        if (elements.length < 3) {
            return Integer.toString(attr.getUId());
        } else {
            return elements[2];
        }
    }

    private String getGroup(String longName, SftpATTRS attr) {

        String[] elements = StringUtils.split(longName);
        if (elements.length < 4) {
            return Integer.toString(attr.getUId());
        } else {
            return elements[3];
        }
    }

    @Override
    public FileStatus getFileStatus(Path file, Set<FileStatus> dirContentList)
            throws IOException {
        Path parentPath = file.getParent();
        // Get the special treatment for root directory
        if (parentPath == null) {
            // Length of root directory on server not known
            return AbstractFTPFileSystem.getRootStatus(getConnectionInfo().getURI());
        }
        String pathName = parentPath.toUri().getPath();
        List<ChannelSftp.LsEntry> sftpFiles;
        try {
            // Get all items from the parent directory
            sftpFiles = client.ls(pathName);
        } catch (SftpException e) {
            LOG.debug("Error when listing files", e);
            if (e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
                throw new FileNotFoundException(String.format(
                        ErrorStrings.E_FILE_NOTFOUND, file));
            } else {
                throw new IOException(e.toString(), e);
            }
        }
        FileStatus fileStat = null;
        if (sftpFiles != null) {
            Path qParent = parentPath.makeQualified(getConnectionInfo().getURI(),
                    parentPath);
            for (ChannelSftp.LsEntry sftpFile : sftpFiles) {
                FileStatus item = getFileStatus(sftpFile, parentPath);
                // Some servers may return "." and ".." which we don't want to process
                if (!item.getPath().equals(qParent) &&
                        !item.getPath().equals(qParent.getParent())) {
                    dirContentList.add(item);
                }
                if (sftpFile.getFilename().equals(file.getName())) {
                    // That's the file we are querying
                    fileStat = getFileStatus(sftpFile, parentPath);
                }
            }
            if (fileStat == null) {
                throw new FileNotFoundException(String.format(
                        ErrorStrings.E_FILE_NOTFOUND, file));
            }
        } else {
            throw new FileNotFoundException(
                    String.format(ErrorStrings.E_FILE_NOTFOUND, file));
        }
        return fileStat;
    }

    /**
     * Return file permission.
     *
     * @param sftpFile file information as received from remote server
     * @return file permission
     */
    private static FsPermission getPermissions(ChannelSftp.LsEntry sftpFile) {
        return new FsPermission((short) sftpFile.getAttrs().getPermissions());
    }

    @Override
    public FSDataInputStream get(FileStatus file,
                                 FileSystem.Statistics statistics) throws IOException {
        LOG.debug("Getting data stream for: " + file.getPath());
        // Get the data stream
        InputStream is = getDataStream(file);
        if (is != null) {
            // All extra handling is done in SFTPInputStream
            return new FSDataInputStream(new SFTPInputStream(is, this, file, statistics));
        } else {
            throw new IOException(String.format(ErrorStrings.E_CREATE_FILE,
                    file.getPath()));
        }
    }

    @Override
    public InputStream getDataStream(FileStatus file) throws IOException {
        try {
            return client.get(file.getPath().toUri().getPath());
        } catch (SftpException ex) {
            throw new IOException(ex.toString(), ex);
        }
    }

    @Override
    public void setTimes(Path p, long mtime, long atime) throws IOException {
        try {
            Date d = new Date(mtime);
            client.setMtime(p.toUri().getPath(), (int) d.toInstant().getEpochSecond());
        } catch (SftpException ex) {
            throw new FileNotFoundException(ex.toString());
        }
    }
}
