package com.superior.datatunnel.hadoop.fs.ftpextended.ftp;

import java.io.IOException;

import com.superior.datatunnel.hadoop.fs.ftpextended.common.AbstractChannel;
import com.superior.datatunnel.hadoop.fs.ftpextended.common.AbstractFTPFileSystem;
import com.superior.datatunnel.hadoop.fs.ftpextended.common.Channel;
import com.superior.datatunnel.hadoop.fs.ftpextended.common.ConnectionInfo;

import java.util.function.Function;

/**
 * FTP FileSystem.
 */
public class FTPFileSystem extends AbstractFTPFileSystem {

    private static final int DEFAULT_FTP_PORT = 21;

    @Override
    protected int getDefaultPort() {
        return DEFAULT_FTP_PORT;
    }

    @Override
    public Channel connect() throws IOException {
        return super.connect();
    }

    @Override
    protected Function<ConnectionInfo, ? extends AbstractChannel> getChannelSupplier() {
        return FTPChannel::create;
    }
}
