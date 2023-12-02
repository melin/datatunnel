package com.superior.datatunnel.hadoop.fs.ftpextended.ftp;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.superior.datatunnel.hadoop.fs.ftpextended.common.Server;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.Authentication;
import org.apache.ftpserver.ftplet.AuthenticationFailedException;
import org.apache.ftpserver.ftplet.Authority;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.listener.Listener;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.ssl.SslConfiguration;
import org.apache.ftpserver.ssl.SslConfigurationFactory;
import org.apache.ftpserver.usermanager.UsernamePasswordAuthentication;
import org.apache.ftpserver.usermanager.impl.AbstractUserManager;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.ConcurrentLoginPermission;
import org.apache.ftpserver.usermanager.impl.WritePermission;

import static com.superior.datatunnel.hadoop.fs.ftpextended.ftp.ITestFTPFileSystem.PASSWORD;
import static com.superior.datatunnel.hadoop.fs.ftpextended.ftp.ITestFTPFileSystem.USER;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;

/**
 * FTPS test server.
 */
public class FTPSServer implements Server {
    private static final String TESTPASS = "testpass";

    private final FtpServer server;

    private final FtpServerFactory serverFactory;

    private final int port;

    private static KeyPair generateKeyPair(int keySize)
            throws NoSuchAlgorithmException, NoSuchProviderException {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        SecureRandom secureRandom = SecureRandom
                .getInstance("SHA1PRNG");
        generator.initialize(keySize, secureRandom);
        return generator.generateKeyPair();
    }

    private static SslConfigurationFactory getSSLConfiguration()
            throws IOException {
        SslConfigurationFactory ssl = new SslConfigurationFactory();
        try {
            KeyPair keyPair = generateKeyPair(1024);
            PrivateKey rootPrivateKey = keyPair.getPrivate();
            Certificate[] chain = new Certificate[1];
            chain[0] = KeyStoreTestUtil.generateCertificate("CN=Test CA Certificate",
                    keyPair, 1, "SHA1WITHRSA");

            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(null);
            ks.setKeyEntry("alias", rootPrivateKey, TESTPASS.toCharArray(), chain);

            File f = File.createTempFile("test", "store");
            f.deleteOnExit();
            try (FileOutputStream fos = new FileOutputStream(f)) {
                ks.store(fos, TESTPASS.toCharArray());
            }

            ssl.setKeystorePassword(TESTPASS);
            ssl.setKeystoreFile(f);
        } catch (Exception e) {
            throw new IOException(e.toString(), e);
        }
        return ssl;
    }

    public FTPSServer(String root) throws IOException, FtpException {
        serverFactory = new FtpServerFactory();
        SslConfigurationFactory ssl = getSSLConfiguration();

        ListenerFactory factory = new ListenerFactory();
        // set the port of the listener
        factory.setPort(0);
        SslConfiguration sslConf = ssl.createSslConfiguration();
        factory.setSslConfiguration(sslConf);

        // replace the default listener
        Listener l = factory.createListener();
        serverFactory.addListener("default", l);

        List<Authority> auth = new ArrayList<>();
        auth.add(new WritePermission());
        auth.add(new ConcurrentLoginPermission(1, 5));
        UserManager um = new AbstractUserManager("admin", null) {
            @Override
            public User getUserByName(String string) throws FtpException {
                return null;
            }

            @Override
            public String[] getAllUserNames() throws FtpException {
                return new String[]{USER};
            }

            @Override
            public void delete(String string) throws FtpException {
            }

            @Override
            public void save(User user) throws FtpException {
            }

            @Override
            public boolean doesExist(String string) throws FtpException {
                return USER.endsWith(string);
            }

            @Override
            public User authenticate(Authentication a) throws
                    AuthenticationFailedException {
                UsernamePasswordAuthentication u = (UsernamePasswordAuthentication) a;
                if (u.getUsername().equals(USER) && u.getPassword().equals(PASSWORD)) {
                    BaseUser b = new BaseUser();
                    b.setName(USER);
                    b.setPassword(PASSWORD);
                    b.setHomeDirectory(root);
                    b.setAuthorities(auth);
                    return b;
                } else {
                    throw new AuthenticationFailedException();
                }
            }
        };
        serverFactory.setUserManager(um);
        // start the server
        server = serverFactory.createServer();
        server.start();
        port = l.getPort();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ex) {
            Logger.getLogger(FTPSServer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public void stop() {
        server.stop();
    }

    @Override
    public FtpServerFactory getServerFactory() {
        return serverFactory;
    }
}
