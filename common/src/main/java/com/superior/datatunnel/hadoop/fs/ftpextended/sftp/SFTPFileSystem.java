package com.superior.datatunnel.hadoop.fs.ftpextended.sftp;

import java.util.function.Function;

import com.superior.datatunnel.hadoop.fs.ftpextended.common.AbstractChannel;
import com.superior.datatunnel.hadoop.fs.ftpextended.common.AbstractFTPFileSystem;
import com.superior.datatunnel.hadoop.fs.ftpextended.common.ConnectionInfo;

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
    protected Function<ConnectionInfo, ? extends AbstractChannel>
    getChannelSupplier() {
        return SFTPChannel::create;
    }
}
