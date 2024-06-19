package com.superior.datatunnel.hadoop.fs.common;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.util.List;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.security.alias.AbstractJavaKeyStoreProvider;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities to obtain transparently passwords and keys.
 */
public final class KeyUtils {
    private static final Logger LOG = LoggerFactory.getLogger(KeyUtils.class);

    private KeyUtils() {}

    static byte[] getKey(ConnectionInfo info, String path) {
        byte[] key = null;
        if (path != null) {
            key = getKeyFromFile(info, path);
        }
        if (key == null) {
            return getKeyFromKeystore(info);
        } else {
            return key;
        }
    }

    static char[] getKeyPassphrase(ConnectionInfo info) {
        try {
            return info.getConf().getPassword(info.getFtpHost() + "_" + info.getFtpUser() + "_key_passphrase");
        } catch (IOException ex) {
            return null;
        }
    }

    private static byte[] getKeyFromFile(ConnectionInfo info, String path) {
        try {
            URI keyURI = new URI(path);
            FileSystem fs = FileSystem.get(keyURI, info.getConf());
            try (FSDataInputStream dis = fs.open(new Path(keyURI))) {
                byte[] buffer = new byte[dis.available()];
                dis.readFully(0, buffer);
                return buffer;
            }
        } catch (IOException | URISyntaxException ex) {
            LOG.error(info.logWithInfo("Key can't be obtained"), ex);
        }
        return null;
    }

    /**
     * Return Private key associated with the ftp user.
     * Key store key used is user_key
     */
    private static byte[] getKeyFromKeystore(ConnectionInfo info) {
        try {
            List<CredentialProvider> providers = CredentialProviderFactory.getProviders(info.getConf());
            for (CredentialProvider provider : providers) {
                if (provider instanceof AbstractJavaKeyStoreProvider) {
                    AbstractJavaKeyStoreProvider jks = (AbstractJavaKeyStoreProvider) provider;
                    KeyStore ks = jks.getKeyStore();
                    Key key =
                            ks.getKey(info.getFtpHost() + "_" + info.getFtpUser() + "_key", getKeystorePassword(info));
                    if (key != null) {
                        StringWriter stringWriter = new StringWriter();
                        try (PemWriter pemWriter = new PemWriter(stringWriter)) {
                            pemWriter.writeObject(new PemObject("PRIVATE KEY", key.getEncoded()));
                        }
                        return stringWriter.toString().getBytes(StandardCharsets.UTF_8);
                    }
                }
            }
        } catch (IOException | KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException ex) {
            LOG.error(info.logWithInfo("Key can't be obtained"), ex);
        }
        return null;
    }

    /**
     * Returns password associated with the key store.
     */
    private static char[] getKeystorePassword(ConnectionInfo info) throws IOException {
        char[] password = ProviderUtils.locatePassword(
                AbstractJavaKeyStoreProvider.CREDENTIAL_PASSWORD_ENV_VAR,
                info.getConf().get(AbstractJavaKeyStoreProvider.CREDENTIAL_PASSWORD_FILE_KEY));
        if (password == null) {
            password = AbstractJavaKeyStoreProvider.CREDENTIAL_PASSWORD_DEFAULT.toCharArray();
        }
        return password;
    }
}
