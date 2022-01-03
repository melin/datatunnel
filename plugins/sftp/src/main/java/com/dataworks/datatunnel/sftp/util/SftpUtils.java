package com.dataworks.datatunnel.sftp.util;

import com.dataworker.spark.jobserver.api.LogUtils;
import com.jcraft.jsch.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * https://medium.com/@princebatra2315/upload-and-download-a-file-through-sftp-in-java-8bde3b8e4bdb
 * @author melin 2021/8/18 2:51 下午
 */
public class SftpUtils {

    private static final String STR_STRICT_HOST_KEY_CHECKING = "StrictHostKeyChecking";

    private static final String STR_SFTP = "sftp";

    private static final String STR_NO = "no";

    public static ChannelSftp setupJsch(SparkSession sparkSession, Map<String, String> options) throws IOException {
        try {
            String username = options.get("username");
            String password = options.get("password");
            String host = options.get("host");
            String port = options.get("port");

            // 私钥文件的路径
            String keyFilePath = options.get("keyFilePath");
            keyFilePath = loadLeyFilePathToLocal(sparkSession, keyFilePath);
            // 密钥的密码
            String passPhrase = options.get("passPhrase");

            int sftpPort = 22;
            if (StringUtils.isNotBlank(port)) {
                sftpPort = Integer.parseInt(port);
            }

            JSch jsch = new JSch();

            boolean useIdentity = keyFilePath != null && !keyFilePath.isEmpty();
            if (useIdentity) {
                LogUtils.info(sparkSession, "秘钥认证");
                if (passPhrase != null) {
                    jsch.addIdentity(keyFilePath, passPhrase);
                } else {
                    jsch.addIdentity(keyFilePath);
                }
            }

            Session session = jsch.getSession(username, host, sftpPort);
            session.setConfig(STR_STRICT_HOST_KEY_CHECKING, STR_NO);
            if (!useIdentity) {
                LogUtils.info(sparkSession, "密码认证");
                session.setPassword(password);
            }
            session.connect();

            ChannelSftp channel = (ChannelSftp) session.openChannel(STR_SFTP);
            channel.connect();

            return channel;
        } catch (JSchException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    /**
     * hdfs 文件写入本地
     *
     * @param sparkSession
     * @param keyFilePath
     * @return 本地文件路径
     * @throws IOException
     */
    private static String loadLeyFilePathToLocal(SparkSession sparkSession, String keyFilePath) throws IOException {
        if (StringUtils.isNotBlank(keyFilePath)) {
            Configuration configuration = sparkSession.sparkContext().hadoopConfiguration();
            FileSystem fileSystem = FileSystem.get(configuration);
            Path path = new Path(keyFilePath);
            if (fileSystem.exists(path)) {
                FSDataInputStream fin = fileSystem.open(path);

                File tempFile = File.createTempFile("datax_", ".key");
                tempFile.deleteOnExit();
                FileOutputStream fos = new FileOutputStream(tempFile);

                IOUtils.copyBytes(fin, fos, 4096, true);

                return tempFile.getPath();
            } else {
                LogUtils.warn(sparkSession, "文件不存在: " + keyFilePath);
            }
        }

        return null;
    }

    public static boolean checkFileExists(ChannelSftp sftp, String path) throws IOException {
        try {
            sftp.lstat(path);
            return true;
        } catch (SftpException e) {
            return false;
        }
    }

    public static boolean mkdir(ChannelSftp sftp, String path) throws IOException {
        try {
            sftp.mkdir(path);
            return true;
        } catch (SftpException e) {
            return false;
        }
    }

    public static boolean delete(ChannelSftp sftp, String path) throws IOException {
        try {
            sftp.rm(path);
            return true;
        } catch (SftpException e) {
            return false;
        }
    }

    public static boolean upload(ChannelSftp sftp, InputStream inputStream, String path) throws IOException {
        try {
            sftp.put(inputStream, path);
            return true;
        } catch (SftpException e) {
            return false;
        }
    }

    public static boolean rename(ChannelSftp sftp, String oldPath, String newPath) throws IOException {
        try {
            sftp.rename(oldPath, newPath);
            return true;
        } catch (SftpException e) {
            return false;
        }
    }

    public static void close(ChannelSftp sftp) throws IOException {
        try {
            sftp.getSession().disconnect();
            sftp.disconnect();
        } catch (JSchException e) {
            throw new IOException(e.getMessage(), e);
        }
    }
}
