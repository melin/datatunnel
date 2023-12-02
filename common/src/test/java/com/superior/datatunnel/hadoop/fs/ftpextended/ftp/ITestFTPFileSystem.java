package com.superior.datatunnel.hadoop.fs.ftpextended.ftp;

import java.io.IOException;
import java.net.URI;

import com.superior.datatunnel.hadoop.fs.ftpextended.common.AbstractFTPFileSystemTest;
import com.superior.datatunnel.hadoop.fs.ftpextended.common.Server;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test functionality of FTP file system.
 */
@RunWith(Parameterized.class)
public class ITestFTPFileSystem extends AbstractFTPFileSystemTest {

    static final URI FTP_URI = URI.create("ftp://user:password@localhost");

    static final String USER = "user";

    static final String PASSWORD = "password";

    private static Server server;

    @BeforeClass
    public static void setTest() throws IOException, FtpException {
        server = new FTPServer(TEST_ROOT_DIR);
    }

    @AfterClass
    public static void cleanTest() {
        server.stop();
    }

    @Before
    @Override
    public void setup() throws IOException {
        Configuration conf = new Configuration();
        conf.setClass("fs.ftp.impl", FTPFileSystem.class, FileSystem.class);
        conf.setInt("fs.ftp.host.port", server.getPort());
        conf.setBoolean("fs.ftp.impl.disable.cache", true);
        conf.setBoolean("fs.ftp.cache." + FTP_URI.getHost(), cache);
        setFS(FTP_URI, conf);
        super.setup();
    }
}
