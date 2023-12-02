package com.superior.datatunnel.hadoop.fs.ftpextended.sftp;

import java.io.File;
import java.io.IOException;
import java.security.KeyPair;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.superior.datatunnel.hadoop.fs.ftpextended.common.Server;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.common.config.keys.KeyUtils;
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import org.apache.sshd.common.io.IoSession;
import org.apache.sshd.common.io.IoWriteFuture;
import org.apache.sshd.common.keyprovider.ClassLoadableResourceKeyPairProvider;
import org.apache.sshd.common.keyprovider.KeyPairProvider;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.auth.UserAuth;
import org.apache.sshd.server.auth.password.UserAuthPasswordFactory;
import org.apache.sshd.server.auth.pubkey.PublickeyAuthenticator;
import org.apache.sshd.server.auth.pubkey.UserAuthPublicKeyFactory;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.session.ServerSession;
import org.apache.sshd.server.session.ServerSessionImpl;
import org.apache.sshd.server.session.SessionFactory;
import org.apache.sshd.server.subsystem.sftp.SftpSubsystemFactory;

/**
 * SFTP test server.
 */
public class SFTPServer implements Server {
    private static SshServer sshd = null;

    private static int port;

    private Iterable<KeyPair> pairRsa = createTestHostKeyProvider().loadKeys();

    private PublickeyAuthenticator delegate  = (username, key, session) -> {
        String fp = KeyUtils.getFingerPrint(key);
        for (KeyPair pair : pairRsa) {
            if (key.equals(pair.getPublic())) {
                return true;
            }
        }
        return false;
    };

    public SFTPServer(String root) throws IOException {
        sshd = SshServer.setUpDefaultServer();
        // ask OS to assign a port
        sshd.setPort(0);
        sshd.setSessionFactory(new SessionFactory(sshd) {
            @Override
            protected ServerSessionImpl doCreateSession(IoSession ioSession) throws
                    Exception {
                return new ServerSessionImpl(this.getServer(), ioSession) {
                    @Override
                    protected IoWriteFuture sendIdentification(String ident) throws IOException {
                        try {
                            // wait a bit so connection is established before sending ident
                            // Seems to be a bug in littleproxy
                            Thread.sleep(50); // NOSONAR
                        } catch (InterruptedException ex) {
                        }
                        return super.sendIdentification(ident);
                    }
                };
            }
        });
        sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider());

        File rootDir = new File(root);
        rootDir.mkdir();
        sshd.setFileSystemFactory(new VirtualFileSystemFactory(
                rootDir.toPath()));
        List<NamedFactory<UserAuth>> userAuthFactories = new ArrayList<>();
        userAuthFactories.add(new UserAuthPasswordFactory());
        userAuthFactories.add(new UserAuthPublicKeyFactory());

        sshd.setUserAuthFactories(userAuthFactories);
        sshd.setPublickeyAuthenticator((String username,
                                        PublicKey key,
                                        ServerSession session) ->
                delegate.authenticate(username, key, session));
        sshd.setPasswordAuthenticator((String username,
                                       String password,
                                       ServerSession session) ->
                "user".equals(username) && "password".equals(password)
        );
        sshd.setSubsystemFactories(
                Arrays.<NamedFactory<Command>>asList(new SftpSubsystemFactory()));

        sshd.start();
        port = sshd.getPort();
    }

    @Override
    public void stop() {
        if (sshd != null) {
            try {
                sshd.stop();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public FtpServerFactory getServerFactory() {
        throw new UnsupportedOperationException("Not supported for this server.");
    }

    private static final AtomicReference<ClassLoadableResourceKeyPairProvider>
            KEYPAIR_PROVIDER_HOLDER = new AtomicReference<>();

    private static KeyPairProvider createTestHostKeyProvider() {
        ClassLoadableResourceKeyPairProvider provider =
                KEYPAIR_PROVIDER_HOLDER.get();
        if (provider != null) {
            return provider;
        }

        provider = new ClassLoadableResourceKeyPairProvider(
                SFTPServer.class.getClassLoader(),
                Arrays.asList("test-user-pass", "test-user"));
        provider.setPasswordFinder((
                String resourceKey) -> {
            return "test-user-pass".equals(resourceKey) ? "passphrase" : null;
        });

        KeyPairProvider prev = KEYPAIR_PROVIDER_HOLDER.getAndSet(provider);
        if (prev != null) { // check if somebody else beat us to it
            return prev;
        } else {
            return provider;
        }
    }
}
