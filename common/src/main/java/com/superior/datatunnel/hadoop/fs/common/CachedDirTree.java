package com.superior.datatunnel.hadoop.fs.common;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;

/**
 * DirTree implementation which caches access to files and directories. As there
 * is no mechanism how to monitor remote file system changes, the information in
 * the cache is valid in the moment of adding nodes but can't be considered as
 * "absolute" true at any moment later. That's not problem for intended use -
 * distcp operations - when we need to traverse tree of files at the beginning
 * and reuse this information later.
 */
public class CachedDirTree implements DirTree {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(CachedDirTree.class);

    private final INode root;

    private final URI uri;

    public CachedDirTree(URI uri) {
        this.uri = uri;
        this.root = new Node();
    }

    @Override
    public INode addNode(Channel channel, Path p) throws IOException {
        Path parent = p.getParent();
        if (parent == null) {
            return root;
        }
        // Recursively get to the path root and then come back while adding
        // all files in the traverse directory structure to the cache
        Node n = (Node) addNode(channel, parent);
        if (!n.nodes.containsKey(p.getName())) {
            // if the added node is not already in th cache than add it with
            // all the files on the same directory level
            HashSet<FileStatus> dirContentList = new HashSet<>();
            FileStatus status = channel.getFileStatus(p, dirContentList);
            n.addAll(dirContentList.toArray(new FileStatus[dirContentList.size()]));
            // We check the existance of particular file after adding
            // all found to the cache
            if (status == null) {
                throw new FileNotFoundException(String.format("File %s/%s not found", n.status.getPath(), p.getName()));
            }
        }
        return n.nodes.get(p.getName());
    }

    @Override
    public INode findNode(Path p) throws FileNotFoundException {
        Path parent = p.getParent();
        if (parent == null) {
            return root;
        }
        // Recursively get to the path root and then come back returning the node
        // representing found path
        Node n = (Node) findNode(parent);
        if (n == null) {
            // Path was not found in the cache
            return null;
        }
        // Check the parent node for the presence of looked up path
        Node c = n.nodes.get(p.getName());
        if (c == null && n.completed) {
            // looked up path is not in the cache but cache claims to contain
            // all existing paths
            throw new FileNotFoundException(String.format("File %s not found", p.toString()));
        }
        return c;
    }

    @Override
    public boolean removeNode(Path p) {
        Path parent = p.getParent();
        boolean ret = false;
        if (parent != null) {
            try {
                Node n = (Node) findNode(parent);
                if (n != null) {
                    ret = n.nodes.remove(p.getName()) != null;
                } else {
                    LOG.warn("File {} not found in the cache while deleting", p.toString());
                }
            } catch (FileNotFoundException ex) {
                LOG.warn("File {} not found in the cache while deleting", p.toString(), ex);
            }
        }
        return ret;
    }

    private final class Node implements INode {

        private final Map<String, Node> nodes = Collections.synchronizedMap(new HashMap<>());

        private final FileStatus status;

        private boolean completed = false;

        private Node(FileStatus status) {
            this.status = status;
            if (status.isFile()) {
                // Files don't have children so they are completed
                completed = true;
            }
        }

        // Root node
        private Node() {
            status = AbstractFTPFileSystem.getRootStatus(uri);
        }

        @Override
        public void addAll(FileStatus[] files) {
            // If this node is not a directory than we can't add children to it
            if (!status.isDirectory()) {
                throw new IllegalStateException("The file can't contain other files: " + status.getPath());
            }
            // Add all specified files to the node
            for (FileStatus file : files) {
                addNode(this, new Node(file));
            }
            completed = true;
        }

        @Override
        public FileStatus getStatus() {
            return status;
        }

        @Override
        public Collection<INode> getChildren(Channel channel) throws IOException {
            if (!completed && status.isDirectory()) {
                addAll(channel.listFiles(status.getPath()));
            }
            return Collections.unmodifiableCollection(nodes.values());
        }

        @Override
        public boolean isCompleted() {
            return completed;
        }

        private void addNode(Node parent, Node child) {
            nodes.put(child.status.getPath().getName(), child);
        }
    }
}
