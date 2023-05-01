package com.superior.datatunnel.plugin.ftp.fs;

import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.util.DataChecksum;

import java.io.IOException;

/**
 * This class contains constants for configuration keys used
 * in the ftp file system.
 * Note that the settings for unimplemented features are ignored.
 * E.g. checksum related settings are just place holders. Even when
 * wrapped with {@link ChecksumFileSystem}, these settings are not
 * used.
 */
public class FtpConfigKeys extends CommonConfigurationKeys {
    public static final String  BLOCK_SIZE_KEY = "ftp.blocksize";

    public static final long    BLOCK_SIZE_DEFAULT = 4 * 1024;

    public static final String  REPLICATION_KEY = "ftp.replication";

    public static final short   REPLICATION_DEFAULT = 1;

    public static final String  STREAM_BUFFER_SIZE_KEY = "ftp.stream-buffer-size";

    public static final int     STREAM_BUFFER_SIZE_DEFAULT = 1024 * 1024;

    public static final String  BYTES_PER_CHECKSUM_KEY = "ftp.bytes-per-checksum";

    public static final int     BYTES_PER_CHECKSUM_DEFAULT = 512;

    public static final String  CLIENT_WRITE_PACKET_SIZE_KEY = "ftp.client-write-packet-size";

    public static final int     CLIENT_WRITE_PACKET_SIZE_DEFAULT = 64 * 1024;

    public static final boolean ENCRYPT_DATA_TRANSFER_DEFAULT = false;

    public static final long    FS_TRASH_INTERVAL_DEFAULT = 0;

    public static final DataChecksum.Type CHECKSUM_TYPE_DEFAULT = DataChecksum.Type.CRC32;

    public static final String KEY_PROVIDER_URI_DEFAULT = "";

    protected static FsServerDefaults getServerDefaults() throws IOException {
        return new FsServerDefaults(
                BLOCK_SIZE_DEFAULT,
                BYTES_PER_CHECKSUM_DEFAULT,
                CLIENT_WRITE_PACKET_SIZE_DEFAULT,
                REPLICATION_DEFAULT,
                STREAM_BUFFER_SIZE_DEFAULT,
                ENCRYPT_DATA_TRANSFER_DEFAULT,
                FS_TRASH_INTERVAL_DEFAULT,
                CHECKSUM_TYPE_DEFAULT,
                KEY_PROVIDER_URI_DEFAULT);
    }
}
