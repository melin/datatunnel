package com.superior.datatunnel.hadoop.fs.common;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import java.util.function.Function;

import static com.google.common.base.Preconditions.*;

/**
 * Various connection parameters. Default values: PORTS: FTP 21/SFTP 22 PROXY
 * PORTS: HTTP 8080/SOCKS 1080 USE CACHE: true USE KEEPALIVE: false KEEPALIVE
 * PERIOD: 1min MAX CONNECTIONS: 5
 */
public class ConnectionInfo extends Configured {

    private final String ftpUser;

    private final String ftpHost;

    private final int ftpPort;

    private final Function<ConnectionInfo, ? extends AbstractChannel> supplier;

    private final URI uri;

    // Basic default values
    protected static final int DEFAULT_MAX_CONNECTION = 5;

    // Deffault keep alive in minutes
    protected static final int DEFAULT_KEEPALIVE_PERIOD = 1;

    protected static final int DEFAULT_HTTP_PROXY_PORT = 8080;

    protected static final int DEFAULT_SOCKS_PROXY_PORT = 1080;

    protected static final Boolean DEFAULT_USE_KEEPALIVE = false;

    protected static final Boolean DEFAULT_CACHE_DIRECTORIES = false;

    protected static final AbstractFTPFileSystem.ProxyType DEFAULT_PROXY_TYPE = AbstractFTPFileSystem.ProxyType.NONE;

    protected static final AbstractFTPFileSystem.GlobType DEFAULT_GLOB_TYPE = AbstractFTPFileSystem.GlobType.UNIX;

    protected static final String DEFAULT_USER = "anonymous";

    protected static final String DEFAULT_PASSWORD = "anonymous@domain.com";

    private static final String FILE_SYSTEM_PROPERTY = "fs.%s%s";

    /**
     * Configuration parameters which are used on command line as -D properties in
     * the form -Dfs.[ftp|sftp].FSParameter.
     */
    enum FSParameter {
        FS_FTP_USER_PREFIX(".user."),
        FS_FTP_PASSWORD_PREFIX(".password."),
        FS_FTP_HOST(".host"),
        FS_FTP_HOST_PORT(".host.port"),
        FS_FTP_PROXY_TYPE(".proxy.type"),
        FS_FTP_PROXY_HOST(".proxy.host"),
        FS_FTP_PROXY_PORT(".proxy.port"),
        FS_FTP_PROXY_USER(".proxy.user"),
        FS_FTP_PROXY_PASSWORD(".proxy.password"),
        FS_FTP_CONNECTION_MAX(".connections.max"),
        FS_FTP_USE_KEEPALIVE(".use.keepalive"),
        FS_FTP_KEEPALIVE_PERIOD(".keepalive.period"),
        FS_FTP_GLOB_TYPE(".glob.type"),
        FS_FTP_CACHE_DIRECTORIES(".cache."),
        FS_FTP_KEY_PASSPHRASE_PREFIX(".key.passphrase."),
        FS_FTP_KEYFILE_PREFIX(".key.file.");

        private final String value;

        FSParameter(String value) {
            this.value = value;
        }

        private String getValue() {
            return value;
        }
    }

    /**
     * Create connection info from URI and hadoop configuration.
     *
     * @param supplier    supplier which creates instance of the channel
     * @param uriInfo     uri of the server
     * @param conf        hadoop configuration
     * @param defaultPort default port of the server
     * @throws IOException connection problem
     */
    public ConnectionInfo(
            Function<ConnectionInfo, ? extends AbstractChannel> supplier,
            URI uriInfo, Configuration conf, int defaultPort) throws IOException {
        super(conf);
        this.uri = uriInfo;
        this.supplier = supplier;
        // get host information from URI
        String host = uriInfo.getHost();
        // Host specified in file system URI has precedence
        ftpHost = (host == null) ? conf.get(getPropertyName(FSParameter.FS_FTP_HOST,
                uriInfo), null) : host;
        checkNotNull(ftpHost, ErrorStrings.E_HOST_NULL);
        conf.set(getPropertyName(FSParameter.FS_FTP_HOST, uriInfo), ftpHost);

        // Port specified in file system URI has precedence
        int port = uriInfo.getPort();
        ftpPort = (port == -1)
                ? conf.getInt(
                getPropertyName(FSParameter.FS_FTP_HOST_PORT, uriInfo),
                defaultPort)
                : port;
        conf.setInt(
                getPropertyName(FSParameter.FS_FTP_HOST_PORT, uriInfo), ftpPort);

        // get user/password information from URI
        String userAndPwdFromUri = uriInfo.getUserInfo();
        if (userAndPwdFromUri != null) {
            String[] userPasswdInfo = userAndPwdFromUri.split(":");
            String user = userPasswdInfo[0];
            user = URLDecoder.decode(user, "UTF-8");
            conf.set(getPropertyName(FSParameter.FS_FTP_USER_PREFIX, uriInfo) +
                    ftpHost, user);
            if (userPasswdInfo.length > 1) {
                conf.set(getPropertyName(FSParameter.FS_FTP_PASSWORD_PREFIX, uriInfo) +
                        ftpHost + "." +
                        user, userPasswdInfo[1]);
            }
        }

        ftpUser = conf.get(
                getPropertyName(FSParameter.FS_FTP_USER_PREFIX, uriInfo) + ftpHost);
        checkArgument(ftpUser != null && !ftpUser.isEmpty(),
                ErrorStrings.E_USER_NULL);
    }

    /**
     * Generates command line property name from FSParameter.
     *
     * @param name parameter we want to translate to property name
     * @return string representation of a property which is used on command line
     * in -Dproperty=value
     */
    static String getPropertyName(FSParameter name, URI uri) {
        return String.format(FILE_SYSTEM_PROPERTY,
                uri.getScheme(), name.getValue());
    }

    /**
     * Supplier which creates instance of the channel.
     *
     * @return Supplier which creates instance of the channel
     */
    Function<ConnectionInfo, ? extends AbstractChannel> getConnectionSupplier() {
        return supplier;
    }

    /**
     * URI associated with this ConnectionInfo.
     *
     * @return URI associated with this ConnectionInfo
     */
    public URI getURI() {
        return uri;
    }

    @Override
    public String toString() {
        return ftpUser + "@" + ftpHost + ":" + ftpPort;
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        if (ftpHost != null) {
            hashCode += ftpHost.hashCode();
        }
        hashCode += ftpPort;
        if (ftpUser != null) {
            hashCode += ftpUser.hashCode();
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ConnectionInfo other = (ConnectionInfo) obj;

        if (!Objects.equals(this.ftpHost, other.ftpHost)) {
            return false;
        }
        if (this.ftpPort != other.ftpPort) {
            return false;
        }

        return Objects.equals(this.ftpUser, other.ftpUser);
    }

    public AbstractFTPFileSystem.ProxyType getProxyType() {
        return getConf().getEnum(
                getPropertyName(FSParameter.FS_FTP_PROXY_TYPE, uri),
                AbstractFTPFileSystem.ProxyType.NONE);
    }

    public String getProxyHost() {
        return getConf().get(getPropertyName(FSParameter.FS_FTP_PROXY_HOST, uri));
    }

    public int getProxyPort() {
        AbstractFTPFileSystem.ProxyType proxyType = getProxyType();
        return getConf().getInt(getPropertyName(FSParameter.FS_FTP_PROXY_PORT, uri),
                proxyType == AbstractFTPFileSystem.ProxyType.HTTP
                        ? DEFAULT_HTTP_PROXY_PORT
                        : ((proxyType == AbstractFTPFileSystem.ProxyType.SOCKS4 || proxyType ==
                        AbstractFTPFileSystem.ProxyType.SOCKS5) ? DEFAULT_SOCKS_PROXY_PORT : -1));
    }

    public String getProxyUser() {
        return getConf().get(getPropertyName(FSParameter.FS_FTP_PROXY_USER, uri));
    }

    public String getProxyPassword() {
        return getConf().get(
                getPropertyName(FSParameter.FS_FTP_PROXY_PASSWORD, uri));
    }

    public String getFtpHost() {
        return ftpHost;
    }

    public int getFtpPort() {
        return ftpPort;
    }

    public String getFtpUser() {
        return ftpUser;
    }

    public String getFtpPassword() {
        String pass = getConf().get(
                getPropertyName(FSParameter.FS_FTP_PASSWORD_PREFIX, uri) + ftpHost +
                        "." + ftpUser);
        try {
            if (pass == null) {
                char[] p = getConf().getPassword(ftpHost + "_" + ftpUser + "_password");
                pass = p == null ? null : new String(p);
            }
            return pass;
        } catch (IOException ex) {
            return null;
        }
    }

    public int getMaxConnections() {
        return getConf().getInt(getPropertyName(FSParameter.FS_FTP_CONNECTION_MAX,
                uri), DEFAULT_MAX_CONNECTION);
    }

    public int getKeepAlivePeriod() {
        return getConf().getInt(getPropertyName(FSParameter.FS_FTP_KEEPALIVE_PERIOD,
                uri), DEFAULT_KEEPALIVE_PERIOD);
    }

    public boolean isCacheDirectories() {
        return getConf().getBoolean(getPropertyName(
                        FSParameter.FS_FTP_CACHE_DIRECTORIES, uri) + ftpHost,
                DEFAULT_CACHE_DIRECTORIES);
    }

    public boolean isUseKeepAlive() {
        return getConf().getBoolean(
                getPropertyName(FSParameter.FS_FTP_USE_KEEPALIVE, uri),
                DEFAULT_USE_KEEPALIVE);
    }

    public AbstractFTPFileSystem.GlobType getGlobType() {
        return getConf().getEnum(getPropertyName(FSParameter.FS_FTP_GLOB_TYPE, uri),
                AbstractFTPFileSystem.GlobType.UNIX);
    }

    public String logWithInfo(String message) {
        return "[" + toString() + "]: " + message;
    }

    public byte[] getKey() {
        return KeyUtils.getKey(this,
                getConf().get(getPropertyName(FSParameter.FS_FTP_KEYFILE_PREFIX,
                        uri) + ftpHost + "." + ftpUser));
    }

    public byte[] getKeyPassPhrase() {
        String passphrase = getConf().get(
                getPropertyName(FSParameter.FS_FTP_KEY_PASSPHRASE_PREFIX, uri)
                        + ftpHost + "." + ftpUser);
        if (passphrase == null) {
            char[] p = KeyUtils.getKeyPassphrase(this);
            return p == null ? null : new String(p).getBytes(StandardCharsets.UTF_8);
        } else {
            return passphrase.getBytes(StandardCharsets.UTF_8);
        }
    }
}
