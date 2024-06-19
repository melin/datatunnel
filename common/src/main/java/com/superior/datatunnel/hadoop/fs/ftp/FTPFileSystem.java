package com.superior.datatunnel.hadoop.fs.ftp;

import com.superior.datatunnel.hadoop.fs.common.AbstractChannel;
import com.superior.datatunnel.hadoop.fs.common.AbstractFTPFileSystem;
import com.superior.datatunnel.hadoop.fs.common.Channel;
import com.superior.datatunnel.hadoop.fs.common.ConnectionInfo;
import java.io.IOException;
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
