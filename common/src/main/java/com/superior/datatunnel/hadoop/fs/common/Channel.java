package com.superior.datatunnel.hadoop.fs.common;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Abstraction of simple operations on top of a concrete file system client.
 * Implementation communicates with the remote server to perform the operation
 */
public interface Channel extends Closeable {
    // Default blocks size - we don't know it so lets use 4kB

    int DEFAULT_BLOCK_SIZE = 4 * 1024;

    /**
     * Try some basic checks on top of channel to make sure that channel is in
     * working state. Doesn't cover all possible cases. Only way how to make sure
     * is top perform actual read/write operation.
     *
     * @return true if channel is in able to communicate
     */
    boolean isAvailable();

    /**
     * Check if the channel is connected.
     *
     * @return true if channel is connected
     */
    boolean isConnected();

    /**
     * Disconnect the channel from the server.
     *
     * @throws IOException disconnection fails
     */
    @Override
    void close() throws IOException;

    /**
     * Do the physical disconnection.
     *
     * @throws IOException disconnection fails
     */
    void destroy() throws IOException;

    /**
     * Creates a directory on the remote server. Directory name is created in
     * parentDir if name is relative. If name is absolute parentDir is ignored.
     *
     * @param parentDir directory in which to create directory name
     * @param name      name of a directory to create
     * @return true if operation was successful, false if not
     */
    boolean mkdir(String parentDir, String name);

    /**
     * Renames a directory on remote server.
     *
     * @param oldName Absolute path of the old file
     * @param newName Absolute path of the new file
     * @return true if operation was successful, false if not
     */
    boolean rename(String oldName, String newName);

    /**
     * Remove the file from the remote server.
     *
     * @param file absolute path of the file to be deleted
     * @return true if operation was successful, false if not
     */
    boolean rm(String file);

    /**
     * Remove the directory on the remote server. Only empty directories can be
     * deleted.
     *
     * @param dir absolute path of the directory to be deleted
     * @return true if operation was successful, false if not
     */
    boolean rmdir(String dir);

    /**
     * Get the working directory. We don't keep the status of the directory so
     * home directory is returned.
     *
     * @return working/home directory name
     * @throws IOException connection problem
     */
    String pwd() throws IOException;

    /**
     * Get the status of all files in the remote directory.
     *
     * @param dir absolute path of the directory to query
     * @return array of file statuses
     * @throws IOException connection problem
     */
    FileStatus[] listFiles(Path dir) throws IOException;

    /**
     * Get the status of a particular files. As a by product the status of all
     * files in the parent directory of the queried file are stored to the set
     *
     * @param file           absolute path of the file we query
     * @param dirContentList statuses of all files in the parent directory
     * @return status of the file we are querying
     * @throws IOException connection problem
     */
    FileStatus getFileStatus(Path file, Set<FileStatus> dirContentList) throws IOException;

    /**
     * Get the output stream for storing data on the remote server.
     *
     * @param file       Absolute path of the file to be stored on the remote server
     * @param dirTree    dir cache to update after successful transfer
     * @param statistics Hadoop file system statistics to update
     * @return Output stream
     * @throws IOException connection problem
     */
    FSDataOutputStream put(Path file, DirTree dirTree, FileSystem.Statistics statistics) throws IOException;

    /**
     * Get the input stream for retrieving data from the remote server. This
     * stream can handle the logic of reconnections, timeouts etc.
     *
     * @param file       Absolute name of the file to be retrieved from the remote
     *                   server
     * @param statistics Hadoop file system statistics to update
     * @return Input stream
     * @throws IOException connection problem
     */
    FSDataInputStream get(FileStatus file, FileSystem.Statistics statistics) throws IOException;

    /**
     * Get the native input stream for retrieving data from remote server as
     * provided by connecting client. Usually wrapped in {@link #get} method with
     * the handling logic.
     *
     * @param file Absolute name of the file to be retrieved from the remote
     *             server
     * @return Input stream
     * @throws IOException connection problem
     */
    InputStream getDataStream(FileStatus file) throws IOException;

    /**
     * Returns native client for connecting to the remote server.
     *
     * @param <T> native client used to communicate with remote server
     * @return native client for connecting to the remote server
     */
    <T> T getNative();

    /**
     * Gets the {@link ConnectionInfo} associated with the channel.
     *
     * @return ConnectionInfo associated with the channel
     */
    ConnectionInfo getConnectionInfo();

    /**
     * Query {@link Channel} if it was created by connection pool.
     *
     * @return true if connection was created by the pool
     */
    boolean isPooled();

    /**
     * Return the communication channel to the connection pool if possible. If not
     * close the connection
     *
     * @throws IOException communication problem
     */
    void disconnect() throws IOException;

    /**
     * Depending on hardClose parameter it either tries to return communication
     * channel to the pool either close the connection.
     *
     * @param hardClose if true close the communication channel else try to return
     *                  to the pool (close if not possible)
     * @throws IOException communication problem
     */
    void disconnect(boolean hardClose) throws IOException;

    void setTimes(Path p, long mtime, long atime) throws IOException;
}
