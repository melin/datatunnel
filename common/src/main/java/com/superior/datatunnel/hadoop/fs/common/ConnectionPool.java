package com.superior.datatunnel.hadoop.fs.common;

import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.superior.datatunnel.hadoop.fs.common.ConnectionInfo.DEFAULT_MAX_CONNECTION;

/**
 * ConnectionPool keeps the list of connection to the remote server and reuse
 * them if possible FTP/SFTP connections are expensive to create hence
 * performance is significantly improved if connection is not closed when not
 * used but put into the pool so next operation can reuse it. Channels are
 * grouped by theirs connection information so only channels belonging to the
 * same group are returned. This class is a singleton - only one is created for
 * the application
 */
public final class ConnectionPool {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionPool.class);

    // Static singleton instance of ConnectionPool
    private static ConnectionPool pool = new ConnectionPool(DEFAULT_MAX_CONNECTION);

    // Set of connection groups which were closed and
    // hence can't be used to get new connection
    private final HashSet<ConnectionInfo> closedInfos = new HashSet<>();

    // Store of pooled objects
    private final GenericKeyedObjectPool<ConnectionInfo, Channel> keyedPool = new GenericKeyedObjectPool<>(new ChannelObjectFactory());

    /**
     * Initialize connection pool.
     *
     * @param maxConnection max number of connection to store in pool. Any extra
     *                      connections are possible but not stored there
     */
    private ConnectionPool(int maxConnection) {
        keyedPool.setBlockWhenExhausted(false);
        keyedPool.setMaxTotalPerKey(maxConnection);
        keyedPool.setTestOnReturn(true);
        keyedPool.setTestOnBorrow(true);
    }

    /**
     * used only for testing to clean up connection pool.
     */
    static synchronized void resetPool() {
        pool = new ConnectionPool(DEFAULT_MAX_CONNECTION);
    }

    /**
     * Returns ConnectionPool object.
     *
     * @param maxConnection how many connection to pool at max
     * @return ConenctionPool object
     */
    public static ConnectionPool getConnectionPool(int maxConnection) {
        pool.setMaxConnection(maxConnection);
        return getConnectionPool();
    }

    /**
     * Returns ConnectionPool object without specifying max number of pooled
     * connections.
     *
     * @return ConenctionPool object
     */
    public static ConnectionPool getConnectionPool() {
        return pool;
    }

    /**
     * Gets connection from pool if available.
     *
     * @param info connection information
     * @return communication channel if in pool, null if not
     * @throws IOException communication problem
     */
    private synchronized Channel getFromPool(ConnectionInfo info) throws
            IOException {
        Channel channel;
        if (closedInfos.contains(info)) {
            throw new IOException("File system closed for: " + info.toString());
        }
        try {
            channel = keyedPool.borrowObject(info);
            LOG.debug("Connection obtained from the pool");
        } catch (Exception ex) {
            channel = null;
            LOG.debug("Can't get any new channel from the pool", ex);
        }
        return channel;
    }

    /**
     * Return the unused channel into pool.
     */
    private synchronized void returnToPool(Channel channel) throws IOException {
        if (channel.isPooled()) {
            ConnectionInfo info = channel.getConnectionInfo();
            if (closedInfos.contains(info)) {
                try {
                    // We need to delete the connection
                    // as file system for this info is closed
                    keyedPool.invalidateObject(info, channel);
                } catch (Exception ex) {
                    throw new IOException(ex.toString(), ex);
                }
            } else {
                keyedPool.returnObject(info, channel);
                LOG.debug("Connection returned to the pool");
            }
        } else {
            channel.destroy();
            LOG.debug("Connection discarded");
        }
    }

    /**
     * Remove the lock forbidding creation of new connections for given
     * ConnectionInfo.
     *
     * @param info ConnectionInfo
     */
    public synchronized void init(ConnectionInfo info) {
        // remove the info from closed list
        closedInfos.remove(info);
    }

    /**
     * Shutdown the connection pool for given ConnectionInfo. Existing connections
     * not returned to the pool are not closed until returned to the pool. When
     * new connection is requested exception is thrown.
     *
     * @param info which connections to clean
     */
    public synchronized void shutdown(ConnectionInfo info) {
        keyedPool.clear(info);
        closedInfos.add(info);
        LOG.debug(
                "Pool shutdown for " + info + ". " + keyedPool.getNumActive(info) +
                        " connectionns still active");
    }

    /**
     * Get the current maximum number of channels in th pool.
     *
     * @return the maximum number of communication channels in the pool
     */
    public int getMaxConnection() {
        return keyedPool.getMaxTotalPerKey();
    }

    /**
     * Sets the maximum number of channels in the pool.
     *
     * @param maxConn maximum number of channels
     */
    public void setMaxConnection(int maxConn) {
        keyedPool.setMaxTotalPerKey(maxConn);
    }

    /**
     * Gets the communication channel to server specified by the ConnectionInfo.
     * First try to obtain the channel from connection pool and if none is found
     * than it will try to establish new connection.
     *
     * @param info specification of connection parameters to the remote server
     * @return connected communication channel
     * @throws IOException Connection can't be created
     */
    public Channel connect(ConnectionInfo info) throws IOException {

        LOG.debug("Connection info: " + info.toString());
        Channel channel = getFromPool(info);
        if (channel == null) {
            LOG.debug("Not pooled connection obtained");
            channel = info.getConnectionSupplier().apply(info);
            if (channel == null) {
                throw new IOException("Connection to:" + info.toString() +
                        " can't be created");
            }
        }

        LOG.debug("Current connections after connect:" +
                keyedPool.getNumActive(info));
        return channel;
    }

    /**
     * Handle disconnection of the {@link Channel}. Depending on hardClose
     * parameter and number of connections stored in the pool it either returns
     * the communication channel to the pool without closing either closes the
     * communication channel so it can't be used again
     *
     * @param channel   communication channel to close
     * @param hardClose if true channel is no returned to the poll but closed, if
     *                  false than connection is returned to the pool if there is the space or
     *                  closed if maxConnection limit is reached.
     * @throws IOException communication problem when closing connection
     */
    public void disconnect(Channel channel, boolean hardClose)
            throws IOException {
        if (channel != null) {
            if (hardClose) {
                // Do the real close
                if (channel.isPooled()) {
                    try {
                        keyedPool.invalidateObject(channel.getConnectionInfo(), channel);
                    } catch (Exception ex) {
                        LOG.error(channel.getConnectionInfo()
                                .logWithInfo("Should never happen"), ex);
                    }
                } else {
                    if (channel.isConnected()) {
                        channel.destroy();
                    }
                }
                LOG.debug("Connection closed");
            } else {
                // we can return the channel to the pool
                returnToPool(channel);
            }
        }
        LOG.debug("Connections actively in use: " + keyedPool.getNumActive());
        LOG.debug("Connections waiting in pool: " + keyedPool.getNumIdle());
    }

}
