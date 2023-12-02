package com.superior.datatunnel.hadoop.fs.ftpextended.common;

import org.apache.ftpserver.FtpServerFactory;

/**
 * Base for testing servers.
 */
public interface Server {
    int getPort();

    void stop();

    FtpServerFactory getServerFactory();
}
