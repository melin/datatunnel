package com.superior.datatunnel.hadoop.fs.sftp;

import com.superior.datatunnel.hadoop.fs.common.AbstractChannel;
import com.superior.datatunnel.hadoop.fs.common.AbstractFTPFileSystem;
import com.superior.datatunnel.hadoop.fs.common.ConnectionInfo;
import java.util.function.Function;

/**
 * SFTP FileSystem.
 */
public class SFTPFileSystem extends AbstractFTPFileSystem {

    private static final int DEFAULT_SFTP_PORT = 22;

    @Override
    public int getDefaultPort() {
        return DEFAULT_SFTP_PORT;
    }

    @Override
    protected Function<ConnectionInfo, ? extends AbstractChannel> getChannelSupplier() {
        return SFTPChannel::create;
    }
}
