package com.superior.datatunnel.hadoop.fs.ftpextended.ftp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.superior.datatunnel.hadoop.fs.ftpextended.common.Server;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.Authentication;
import org.apache.ftpserver.ftplet.AuthenticationFailedException;
import org.apache.ftpserver.ftplet.Authority;
import org.apache.ftpserver.ftplet.DefaultFtplet;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpSession;
import org.apache.ftpserver.ftplet.Ftplet;
import org.apache.ftpserver.ftplet.FtpletResult;
import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.listener.Listener;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.UsernamePasswordAuthentication;
import org.apache.ftpserver.usermanager.impl.AbstractUserManager;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.ConcurrentLoginPermission;
import org.apache.ftpserver.usermanager.impl.WritePermission;

import static com.superior.datatunnel.hadoop.fs.ftpextended.ftp.ITestFTPFileSystem.PASSWORD;
import static com.superior.datatunnel.hadoop.fs.ftpextended.ftp.ITestFTPFileSystem.USER;

/**
 * Test FTP server.
 */
public class FTPServer implements Server {
    private final FtpServer server;

    private final FtpServerFactory serverFactory;

    private final int port;

    public FTPServer(String root) throws FtpException {
        serverFactory = new FtpServerFactory();
        ListenerFactory factory = new ListenerFactory();
        // set the port of the listener
        factory.setPort(0);
        // replace the default listener
        Listener l = factory.createListener();
        serverFactory.addListener("default", l);
        Map<String, Ftplet> flets = serverFactory.getFtplets();
        flets.put("t", new DefaultFtplet() {
            @Override
            public FtpletResult onConnect(FtpSession session){
                try {
                    //There seems to be timing issue between FTPServer and LittleProxy
                    //let's go bit slower
                    Thread.sleep(50);
                } catch (InterruptedException ex) {
                }
                return null;
            }
        });

        List<Authority> auth = new ArrayList<>();
        auth.add(new WritePermission());
        auth.add(new ConcurrentLoginPermission(5, 5));
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
