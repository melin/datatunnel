package com.superior.datatunnel.hadoop.fs.ftpextended.sftp;

import com.jcraft.jsch.JSch;

import java.io.IOException;
import java.net.URI;

import com.superior.datatunnel.hadoop.fs.ftpextended.common.AbstractFTPFileSystemTest;
import com.superior.datatunnel.hadoop.fs.ftpextended.common.Server;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Shell;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assume.assumeTrue;

/**
 * Test functionality of SFTP file system.
 */
@RunWith(Parameterized.class)
public class ITestSFTPFileSystem extends AbstractFTPFileSystemTest {
    private static Server server;

    private static final Logger LOG = LoggerFactory.getLogger(ITestSFTPFileSystem.class);

    private static final URI SFTP_URI = URI.create("sftp://user:password@localhost");

    @BeforeClass
    public static void setTest() throws IOException {
        JSch.setLogger(new com.jcraft.jsch.Logger() {
            @Override
            public boolean isEnabled(int i) {
                return true;
            }

            @Override
            public void log(int i, String string) {
                LOG.info(string);
            }
        });
        // skip all tests if running on Windows
        assumeTrue(!Shell.WINDOWS);

        server = new SFTPServer(AbstractFTPFileSystemTest.TEST_ROOT_DIR);
    }

    @AfterClass
    public static void cleantTest() {
        server.stop();
    }

    @Before
    @Override
    public void setup() throws IOException {
        Configuration conf = new Configuration();
        conf.setClass("fs.sftp.impl", SFTPFileSystem.class, FileSystem.class);
        conf.setInt("fs.sftp.host.port", server.getPort());
        conf.setBoolean("fs.sftp.impl.disable.cache", true);
        conf.setBoolean("fs.sftp.cache." + SFTP_URI.getHost(), cache);
        setFS(SFTP_URI, conf);
        super.setup();
    }
}
