package com.superior.datatunnel.hadoop.fs.common;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * Abstraction of file/dir tree caching structure. Each INode represents a file
 * or directory. Each node has completed flag which is true for files and true
 * for directories which content was fully resolved
 */
public interface DirTree {

    /**
     * Adds a node representing path to the tree.
     *
     * @param channel channel used to retrieve status of th path from the remote
     *                server
     * @param p       absolute path of the file/dir to be added
     * @return created node
     * @throws IOException communication problem
     */
    INode addNode(Channel channel, Path p) throws IOException;

    /**
     * Find a path in the tree structure and return node representing it. Doesn't
     * query the remote server.
     *
     * @param p absolute path we want to get node for
     * @return node value or null if node in cash - meaning it needs to be added
     * @throws FileNotFoundException path not found even on completed tree hence
     *                               the file doesn't exists
     */
    INode findNode(Path p) throws FileNotFoundException;

    /**
     * Node to remove from the tree. Used for rm/rmdir/rename operations
     *
     * @param p absolute path of the node to remove
     * @return true if path was deleted from cache, false if not
     */
    boolean removeNode(Path p);

    /**
     * Structure representing a node (file or directory) in the tree. If the node
     * represents a directory than all files/directories it contains can be
     * retrieved by getChildren() method
     */
    public interface INode {

        /**
         * Add status of all files/directories contained in the node. Use of this
         * method implies that completed flag is set to true for this node
         *
         * @param files all files/directories contained in the node
         */
        void addAll(FileStatus[] files);

        /**
         * Returns all file/directories contained in the node. If node is not
         * completed than remote server is queried to update the list and completed
         * flag set to true for cached file system
         *
         * @param channel communication channel to the remote server
         * @return nodes of all children of the node. Empty collection if the node
         * is file
         * @throws IOException communication problem
         */
        Collection<INode> getChildren(Channel channel) throws IOException;

        /**
         * Get the FileStatus of the file/directory this node is representing.
         *
         * @return status of the file/directory this node is representing
         */
        FileStatus getStatus();

        /**
         * Flag describing if all children for given node were found.
         *
         * @return true for file or directory with all children added by using
         * {@link #addAll} or {@link #getChildren} methods
         */
        boolean isCompleted();
    }
}
