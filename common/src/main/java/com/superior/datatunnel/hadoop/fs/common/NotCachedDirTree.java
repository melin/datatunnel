package com.superior.datatunnel.hadoop.fs.common;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * Implementation of DirTree which doesn't do any caching forcing the file
 * system always query the remote file system. Completed flag is always false.
 * Using this caching strategy can cause significant performance hit especially
 * when traversing directories with huge number of files. Suggested use is
 * therefore for using it in "hdfs dfs" operations.
 */
public class NotCachedDirTree implements DirTree {

    @Override
    public INode addNode(Channel channel, Path p) throws IOException {
        return new Node(channel, p);
    }

    @Override
    public INode findNode(Path p) throws FileNotFoundException {
        return null;
    }

    @Override
    public boolean removeNode(Path p) {
        return false;
    }

    private static class Node implements INode {

        private final FileStatus fs;

        Node(Channel channel, Path p) throws IOException {
            // We still have to return node id directly asked
            fs = channel.getFileStatus(p, new HashSet<>());
        }

        @Override
        public void addAll(FileStatus[] files) {
            // No caching
        }

        @Override
        public Collection<INode> getChildren(Channel channel) throws IOException {
            return Collections.emptySet();
        }

        @Override
        public FileStatus getStatus() {
            return fs;
        }

        @Override
        public boolean isCompleted() {
            return false;
        }
    }

}
