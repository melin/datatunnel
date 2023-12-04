package com.superior.datatunnel.hadoop.fs.common;

import java.io.IOException;

/**
 * Class which assures that it's children follows the same creation pattern.
 */
public abstract class AbstractChannel implements Channel {

    private final ConnectionInfo info;

    private boolean isPooled;

    /**
     * Channel must keep {@link ConnectionInfo} to be able to be created.
     *
     * @param info ConenctionInfo for given channel
     */
    protected AbstractChannel(ConnectionInfo info) {
        this.info = info;
        this.isPooled = false;
    }

    @Override
    public void close() throws IOException {
        disconnect(true);
    }

    /**
     * Indicate that channel was created by the connection pool.
     */
    public void setPooled() {
        this.isPooled = true;
    }

    @Override
    public boolean isPooled() {
        return isPooled;
    }

    @Override
    public ConnectionInfo getConnectionInfo() {
        return info;
    }

    /**
     * Depending on hardClose parameter it either tries to return communication
     * channel to the pool either close the connection.
     *
     * @param hardClose if true close the communication channel else try to return
     *                  to the pool (close if not possible)
     * @throws IOException communication problem
     */
    @Override
    public void disconnect(boolean hardClose) throws IOException {
        ConnectionPool.getConnectionPool().disconnect(this, hardClose);
    }

    /**
     * Return the communication channel to the connection pool if possible. If not
     * close the connection
     *
     * @throws IOException communication problem
     */
    @Override
    public void disconnect() throws IOException {
        disconnect(false);
    }
}
