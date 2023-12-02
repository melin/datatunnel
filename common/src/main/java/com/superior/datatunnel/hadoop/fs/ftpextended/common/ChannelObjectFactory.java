package com.superior.datatunnel.hadoop.fs.ftpextended.common;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * Factory for creating pool objects.
 */
class ChannelObjectFactory extends
        BaseKeyedPooledObjectFactory<ConnectionInfo, Channel> {

    @Override
    public Channel create(ConnectionInfo k) throws Exception {
        AbstractChannel channel = k.getConnectionSupplier().apply(k);
        channel.setPooled();
        return channel;
    }

    @Override
    public PooledObject<Channel> wrap(Channel v) {
        return new DefaultPooledObject<>(v);
    }

    @Override
    public void destroyObject(ConnectionInfo key, PooledObject<Channel> p) throws
            Exception {
        Channel channel = p.getObject();
        if (channel.isConnected()) {
            channel.destroy();
        }
    }

    @Override
    public boolean validateObject(ConnectionInfo key, PooledObject<Channel> p) {
        return p.getObject().isConnected() && p.getObject().isAvailable();
    }
}
