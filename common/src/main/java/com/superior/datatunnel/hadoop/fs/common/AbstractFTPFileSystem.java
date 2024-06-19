package com.superior.datatunnel.hadoop.fs.common;

import static com.superior.datatunnel.hadoop.fs.common.Channel.DEFAULT_BLOCK_SIZE;

import com.superior.datatunnel.hadoop.fs.common.DirTree.INode;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.WeakHashMap;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * Abstract base for FTP like FileSystems. Basically all the functionality for
 * both FTP and SFTP file systems is located here - communication details are
 * performed by specialized classes
 */
public abstract class AbstractFTPFileSystem extends FileSystem {

    // Block location for remote file - we don't know anything about it
    // so create empty one
    private static final BlockLocation BLOCK = new BlockLocation();

    // Configuration property defining root for globbed files
    public static final String FS_FTP_GLOB_ROOT_PATH = "fs.ftp.glob.root.path";

    // We need to remove ending slashes as it cause troubles
    private static final Pattern ENDING_SLASH = Pattern.compile("//$");

    // Connection pool associate with the file system. Keeps list of open
    // (but unused) communication channels
    private ConnectionPool connectionPool;

    private URI uri;

    /**
     * Different type of proxies supported by communication channels.
     */
    public enum ProxyType {
        NONE,
        HTTP,
        SOCKS4,
        SOCKS5
    }

    /**
     * How the glob e.g. wildcards are handled.
     */
    protected enum GlobType {
        // UNIX style handling e.g. /dir/*/*.gz will process all gz files from any
        // directory child of directory dir should be used with hdfs dfs operations
        UNIX,
        // Regexp handling e.g /dir/.*gz will process all gz file at any position in
        // the directory tree under dir directory
        // should be used in distcp operations when we want to travers whole subtree
        REGEXP
    }

    // Implementation of directory/file tree cache
    private DirTree dirTree;

    // Cache of home directories for each used channel, should be removed
    // or reimplement if we ever decide to support setWorkingDirectory
    private final WeakHashMap<Channel, Path> workDirs = new WeakHashMap<>();

    private ConnectionInfo connectionInfo;

    /**
     * Get FileStatus of root directory.
     *
     * @param uri server URI
     * @return FileStatus describing root directory
     */
    public static FileStatus getRootStatus(URI uri) {
        // Length of root directory on server not known
        long length = -1;
        boolean isDir = true;
        int blockReplication = 1;
        // Block Size not known.
        long blockSize = DEFAULT_BLOCK_SIZE;
        // Modification time of root directory not known.
        long modTime = -1;
        Path root = new Path("/");
        return new FileStatus(length, isDir, blockReplication, blockSize, modTime, root.makeQualified(uri, root));
    }

    @Override
    public String getScheme() {
        return uri == null ? null : uri.getScheme();
    }

    /**
     * Set configuration from URI and Hadoop configuration. Hadoop configuration
     * comes from parameters specified by hadoop and from command line parameters
     * in form -Dproperty=value
     *
     * @param uriInfo files system URI
     * @param conf    hadoop configuration
     * @throws IOException URI or configuration is invalid
     */
    protected void setConfigurationFromURI(URI uriInfo, Configuration conf) throws IOException {
        uri = uriInfo;
        connectionInfo = new ConnectionInfo(getChannelSupplier(), uri, conf, getDefaultPort());
        connectionPool = ConnectionPool.getConnectionPool(connectionInfo.getMaxConnections());
        connectionPool.init(connectionInfo);
    }

    /**
     * Returns connected communication channel to the remote server.
     *
     * @return Connected communication channel
     * @throws IOException communication channel can't be established
     */
    public Channel connect() throws IOException {
        return connectionPool.connect(connectionInfo);
    }

    /**
     * Returns method which is able to create {@link Channel} for given file
     * system.
     *
     * @return method which is able to create Channel for given file system
     */
    protected abstract Function<ConnectionInfo, ? extends AbstractChannel> getChannelSupplier();

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public void initialize(URI uriInfo, Configuration conf) throws IOException {
        super.initialize(uriInfo, conf);
        setConfigurationFromURI(uriInfo, conf);
        setConf(conf);
        dirTree = connectionInfo.isCacheDirectories() ? new CachedDirTree(uriInfo) : new NotCachedDirTree();
    }

    @Override
    public void close() throws IOException {
        connectionPool.shutdown(connectionInfo);
        super.close();
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        throw new IOException(ErrorStrings.E_NOT_SUPPORTED);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        Channel channel = connect();
        try {
            return rename(channel, src, dst);
        } finally {
            channel.disconnect();
        }
    }

    /**
     * Convenience method, so that we don't open a new connection when using this
     * method from within another method. Otherwise every API invocation incurs
     * the overhead of opening/closing a TCP connection.
     *
     * @param channel communication channel to the remote server
     * @param src     absolute path to the file we want to rename
     * @param dst     absolute path to the file we want to rename to
     * @return true if rename operation was successful
     * @throws IOException communication problem
     */
    protected boolean rename(Channel channel, Path src, Path dst) throws IOException {
        Path absoluteSrc = makeAbsolute(channel, src);
        if (!exists(channel, absoluteSrc)) {
            throw new FileNotFoundException(String.format(ErrorStrings.E_SPATH_NOTEXIST, src));
        }

        Path absoluteDst = makeAbsolute(channel, dst);

        if (exists(channel, absoluteDst)) {
            if (getFileStatus(channel, absoluteDst).isDirectory()) {
                // destination is a directory: rename goes underneath it with the
                // source name
                absoluteDst = new Path(absoluteDst, absoluteSrc.getName());
            }
            if (exists(channel, absoluteDst)) {
                throw new FileAlreadyExistsException(String.format(ErrorStrings.E_DPATH_EXIST, dst));
            }
        }

        if (isParentOf(absoluteSrc, absoluteDst)) {
            throw new IOException("Cannot rename " + absoluteSrc + " under itself" + " : " + absoluteDst);
        }

        boolean renamed = channel.rename(
                absoluteSrc.toUri().getPath(), absoluteDst.toUri().getPath());
        if (renamed) {
            // We need to change the dirTree cache so it contains valid information
            dirTree.removeNode(absoluteSrc);
            dirTree.addNode(channel, absoluteDst);
        }
        return renamed;
    }

    /**
     * Probe for a path being a parent of another.
     *
     * @param parent parent path
     * @param child  possible child path
     * @return true if the parent's path matches the start of the child's
     */
    private boolean isParentOf(Path parent, Path child) {
        URI parentURI = parent.toUri();
        String parentPath = parentURI.getPath();
        if (!parentPath.endsWith("/")) {
            parentPath += "/";
        }
        URI childURI = child.toUri();
        String childPath = childURI.getPath();
        return childPath.startsWith(parentPath);
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        Channel channel = connect();
        try {
            return delete(channel, f, recursive);
        } finally {
            channel.disconnect();
        }
    }

    /**
     * Convenience method, so that we don't open a new connection when using this
     * method from within another method. Otherwise every API invocation incurs
     * the overhead of opening/closing a TCP connection.
     *
     * @param channel   communication channel to the remote server
     * @param file      absolute path to the file/directory we want to delete
     * @param recursive if path denotes directory and set to true than delete
     *                  operation will delete all children
     * @return true if delete operation succeeds
     * @throws IOException communication problem
     */
    protected boolean delete(Channel channel, Path file, boolean recursive) throws IOException {
        Path absolute = makeAbsolute(channel, file);
        FileStatus fileStat;
        try {
            fileStat = getFileStatus(channel, absolute);
        } catch (FileNotFoundException e) {
            // file not found, no need to delete, return false
            LOG.warn(String.format(ErrorStrings.E_FILE_NOTFOUND, file) + " Error: " + e.getMessage());
            return false;
        }
        boolean result;
        String pathName = absolute.toUri().getPath();
        if (!fileStat.isDirectory()) {
            result = channel.rm(pathName);
        } else {
            FileStatus[] dirEntries = listStatus(channel, absolute);
            if (dirEntries != null && dirEntries.length > 0) {
                if (!recursive) {
                    // Can't delete not empty directory
                    throw new IOException(String.format(ErrorStrings.E_DIR_NOTEMPTY, file));
                }
                for (int i = 0; i < dirEntries.length; ++i) {
                    // delete all items from the directory
                    delete(channel, new Path(absolute, dirEntries[i].getPath()), recursive);
                }
            }
            result = channel.rmdir(pathName);
        }
        if (result) {
            // Remove file from the cache only if deletion was successful
            dirTree.removeNode(file);
        }
        return result;
    }

    @Override
    // Permissions are ignored
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        Channel channel = connect();
        try {
            return mkdirs(channel, f);
        } finally {
            channel.disconnect();
        }
    }

    /**
     * Convenience method, so that we don't open a new connection when using this
     * method from within another method. Otherwise every API invocation incurs
     * the overhead of opening/closing a TCP connection.
     *
     * @param channel channel used to communicate with remote server
     * @param dir     absolute path of a directory we want to create
     * @return true if the the directory was created or already exists
     * @throws IOException communication problem or dir is not a directory
     */
    protected boolean mkdirs(Channel channel, Path dir) throws IOException {
        boolean created = true;
        Path absolute = makeAbsolute(channel, dir);
        String pathName = absolute.getName();
        if (!exists(channel, absolute)) {
            Path parent = absolute.getParent();
            // Recursively create all directories in the path
            created = parent == null || mkdirs(channel, parent);
            if (created) {
                String parentDir = parent == null ? "/" : parent.toUri().getPath();
                created = channel.mkdir(parentDir, pathName);
                if (created) {
                    dirTree.addNode(channel, absolute);
                }
            }
        } else if (getFileStatus(channel, absolute).isFile()) {
            throw new IOException(String.format(ErrorStrings.E_DIR_CREATE_FROMFILE, absolute));
        }
        return created;
    }

    /**
     * Returns absolute path of working/home directory associated with the
     * channel.
     *
     * @param channel communication channel
     * @return absolute path of working/home directory
     * @throws IOException communication problem
     */
    public Path getWorkingDirectory(Channel channel) throws IOException {
        if (workDirs.containsKey(channel)) {
            return workDirs.get(channel);
        } else {
            Path homeDir = new Path(channel.pwd());
            workDirs.put(channel, homeDir);
            return homeDir;
        }
    }

    @Override
    public Path getWorkingDirectory() {
        // Return home directory always since we do not maintain state.
        return getHomeDirectory();
    }

    @Override
    public Path getHomeDirectory() {
        Channel channel = null;
        try {
            channel = connect();
            return getWorkingDirectory(channel);
        } catch (IOException ioe) {
            LOG.error(connectionInfo.logWithInfo("Home directory can't be resolved"), ioe);
            return null;
        } finally {
            try {
                if (channel != null) {
                    channel.disconnect();
                }
            } catch (IOException ex) {
                LOG.warn("Disconnection fails, error: " + ex.getMessage());
            }
        }
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        Channel channel = connect();
        try {
            return getFileStatus(channel, f);
        } finally {
            channel.disconnect();
        }
    }

    /**
     * Gets the FileStatus of a remote file specified by its path.
     *
     * @param channel channel to use to communicate with remote server
     * @param f       absolute path of the file we query for the status
     * @return FileStatus
     * @throws IOException when communication problem, FileNotFound when file is
     *                     not found
     */
    protected FileStatus getFileStatus(Channel channel, Path f) throws IOException {
        // Check if we don't have file already in the cache
        DirTree.INode n = dirTree.findNode(f);
        if (n == null) {
            LOG.debug(String.format(ErrorStrings.E_FILE_NOTFOUND, f));
            Path absolute = makeAbsolute(channel, f);
            Path parentPath = absolute.getParent();
            if (parentPath == null) {
                // root directory
                parentPath = new Path("/");
            }
            // Add the parent of the file to the cache
            n = dirTree.addNode(channel, parentPath);
            HashSet<FileStatus> dirContentList = new HashSet<>();
            FileStatus status = channel.getFileStatus(absolute, dirContentList);
            // Add all files form the parent directory to the cache
            n.addAll(dirContentList.toArray(new FileStatus[dirContentList.size()]));
            return status;
        } else {
            return n.getStatus();
        }
    }

    @Override
    public void setWorkingDirectory(Path newDir) {
        // we do not maintain the working directory state
    }

    @Override
    public BlockLocation[] getFileBlockLocations(Path p, long start, long len) throws IOException {
        BlockLocation[] loc = {BLOCK};
        return loc;
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern) throws IOException {
        if (GlobType.UNIX == connectionInfo.getGlobType()) {
            return super.globStatus(pathPattern);
        }
        Path qualifiedPattern = pathPattern.makeQualified(uri, getWorkingDirectory());
        LOG.debug("Start glob processing");
        String path = qualifiedPattern.toUri().getPath();
        Pattern p0 = Pattern.compile(path);
        /*
         * Identify regexp presence in the path. There are 2 groups in this regexp
         * first will contain the path before the start of regexp and we will use it
         * to set the start base for searchinng files on remote file system Second
         * group contains first occurance of the regexp but we don't need it as the
         * complete matching is done on the whole pattern
         */
        String glob = "(.*?)(\\*|\\?|\\(.*\\)|\\[.*\\]|\\{.+,.+\\}).*";
        Pattern p = Pattern.compile(glob);
        Matcher m = p.matcher(path);
        // To avoid duplicates we use Set not an Array
        HashSet<FileStatus> res = new HashSet<>();
        if (m.matches()) {
            // base is a directory before the first rexgexp occurence in the path
            Path globBasePath = new Path(m.group(1));
            if (globBasePath.getParent() != null) {
                globBasePath = globBasePath.getParent();
            }
            getConf().set(FS_FTP_GLOB_ROOT_PATH, globBasePath.toUri().toString());
            RemoteIterator<LocatedFileStatus> list = listFiles(globBasePath, true);
            while (list.hasNext()) {
                FileStatus fs = list.next();
                String file = fs.getPath().toUri().getPath();
                // Match the obtained path with the regexp pattern
                // from the input parameter
                Matcher m0 = p0.matcher(file);
                if (m0.matches()) {
                    // Add matching entries to the result list
                    res.add(fs);
                }
            }
        } else {
            getConf().unset(FS_FTP_GLOB_ROOT_PATH);
            // Input path doesn't contain regular expression
            FileStatus fs = getFileStatus(qualifiedPattern);
            if (fs != null) {
                res.add(fs);
            }
        }

        LOG.debug("Finish glob processing");
        return res.toArray(new FileStatus[res.size()]);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        Channel channel = connect();
        try {
            return listStatus(channel, f);
        } finally {
            channel.disconnect();
        }
    }

    /**
     * Convenience method, so that we don't open a new connection when using this
     * method from within another method. Otherwise every API invocation incurs
     * the overhead of opening/closing a TCP connection.
     *
     * @param channel Communication channel
     * @param file    absolute path of a directory/file whose status we query
     * @return list of file statuses of all files in the directory or status of
     * the file if is not a directory
     * @throws IOException communication problem
     */
    protected FileStatus[] listStatus(Channel channel, Path file) throws IOException {
        // try to find info in the cache
        INode n = dirTree.findNode(file);
        if (n == null || !n.isCompleted()) {
            // not found
            LOG.debug("Complete listing of " + file + " directory not found in cache");
            Path absolute = makeAbsolute(channel, file);
            n = dirTree.addNode(channel, absolute);
            FileStatus fileStat = n.getStatus();
            if (!fileStat.isDirectory()) {
                return new FileStatus[] {fileStat};
            }
            FileStatus[] all = channel.listFiles(absolute);
            n.addAll(all);
            return all;
        } else {
            return n.getStatus().isDirectory()
                    ? n.getChildren(channel).stream()
                            .map(node -> node.getStatus())
                            .toArray(FileStatus[]::new)
                    : new FileStatus[] {n.getStatus()};
        }
    }

    /**
     * Resolve against given working directory.
     *
     * @param channel communication channel to the remote server
     * @param path    path of a file
     * @return absolute path
     * @throws IOException communication problem
     */
    public Path makeAbsolute(Channel channel, Path path) throws IOException {
        if (!path.isRoot()) {
            // remove trailing slashes
            path = new Path(ENDING_SLASH.matcher(path.toString()).replaceAll(""));
        }

        if (path.isAbsolute()) {
            return path;
        }
        Path workDir = getWorkingDirectory(channel);
        return new Path(workDir, path);
    }

    @Override
    public boolean exists(Path f) throws IOException {
        Channel channel = connect();
        try {
            return exists(channel, f);
        } finally {
            channel.disconnect();
        }
    }

    /**
     * Convenience method, so that we don't open a new connection when using this
     * method from within another method. Otherwise every API invocation incurs
     * the overhead of opening/closing a TCP connection.
     *
     * @param channel communication channel to the remote server
     * @param file    path to the file whose existence we are querying
     * @return true if file exists, false in other case
     * @throws IOException communication problem
     */
    protected boolean exists(Channel channel, Path file) throws IOException {
        try {
            getFileStatus(channel, file);
            return true;
        } catch (FileNotFoundException e) {
            LOG.debug(String.format(ErrorStrings.E_FILE_NOTFOUND, file), e);
            return false;
        }
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        Channel channel = connect();
        Path absolute = makeAbsolute(channel, f);
        FileStatus fileStat = getFileStatus(channel, absolute);
        if (fileStat.isDirectory()) {
            // Can't open a path denoting directory
            channel.disconnect();
            throw new FileNotFoundException(String.format(ErrorStrings.E_PATH_DIR, f));
        }
        try {
            // let's traverse symlinks
            while (fileStat.isSymlink()) {
                fileStat = getFileStatus(channel, fileStat.getSymlink());
            }
            return channel.get(fileStat, statistics);
        } catch (IOException e) {
            LOG.error(connectionInfo.logWithInfo(
                    "Can't initialize data connection. " + "Closing the channel and will try again later"));
            channel.disconnect(true);
            throw e;
        }
    }

    /**
     * A stream obtained via this call must be closed before using other APIs of
     * this class or else the invocation will block.
     *
     * @return data stream used for storing file on remote server
     * @throws IOException communication problem
     */
    @Override
    public FSDataOutputStream create(
            Path f,
            FsPermission permission,
            boolean overwrite,
            int bufferSize,
            short replication,
            long blockSize,
            Progressable progress)
            throws IOException {
        final Channel channel = connect();
        if (exists(channel, f)) {
            // let's handle if we can overwrite original file
            FileStatus status = getFileStatus(channel, f);
            if (overwrite && !status.isDirectory()) {
                delete(channel, f, false);
            } else {
                channel.disconnect();
                throw new FileAlreadyExistsException(String.format(ErrorStrings.E_FILE_EXIST, f));
            }
        }

        Path absolute = makeAbsolute(channel, f);
        Path parent = absolute.getParent();
        boolean dirReady = false;
        try {
            dirReady = mkdirs(channel, parent);
        } catch (IOException ex) {
            // when mkdir returns exception return channel to the pool and exit
            channel.disconnect();
            throw ex;
        }
        if (parent == null || !dirReady) {
            // we can't create parent directory for the file
            parent = (parent == null) ? new Path("/") : parent;
            channel.disconnect();
            throw new IOException(String.format(ErrorStrings.E_CREATE_DIR, parent));
        }
        try {
            return channel.put(absolute, dirTree, statistics);
        } catch (IOException e) {
            LOG.error(connectionInfo.logWithInfo("Unreliable channel - closing it"));
            channel.disconnect(true);
            throw e;
        }
    }

    @Override
    public FSDataOutputStream createNonRecursive(
            Path f,
            FsPermission permission,
            EnumSet<CreateFlag> flags,
            int bufferSize,
            short replication,
            long blockSize,
            Progressable progress)
            throws IOException {
        final Channel channel = connect();
        Path absolute = makeAbsolute(channel, f);
        Path parent = absolute.getParent();

        if (parent == null || !exists(channel, parent)) {
            // we can't create parent directory for the file
            parent = (parent == null) ? new Path("/") : parent;
            channel.disconnect();
            throw new IOException(String.format(ErrorStrings.E_FILE_NOTFOUND, parent));
        }

        if (exists(channel, f)) {
            // let's handle if we can overwrite original file
            FileStatus status = getFileStatus(channel, f);
            if (flags.contains(CreateFlag.OVERWRITE) && !status.isDirectory()) {
                delete(channel, f, false);
            } else {
                channel.disconnect();
                throw new FileAlreadyExistsException(String.format(ErrorStrings.E_FILE_EXIST, f));
            }
        }

        try {
            return channel.put(absolute, dirTree, statistics);
        } catch (IOException e) {
            LOG.error(connectionInfo.logWithInfo("Unreliable channel - closing it"));
            channel.disconnect(true);
            throw e;
        }
    }

    @Override
    public void setTimes(Path p, long mtime, long atime) throws IOException {
        Channel channel = connect();
        try {
            channel.setTimes(p, mtime, atime);
        } finally {
            channel.disconnect();
        }
    }
}
