package com.dataworker.datax.sftp.util;

import com.google.common.collect.Maps;
import com.jcraft.jsch.*;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * https://medium.com/@princebatra2315/upload-and-download-a-file-through-sftp-in-java-8bde3b8e4bdb
 * @author melin 2021/8/18 2:51 下午
 */
public class SftpUtils {

    public static ChannelSftp setupJsch(Map<String, String> options) throws IOException {
        try {
            String username = options.get("username");
            String password = options.get("password");
            String host = options.get("host");
            String port = options.get("port");

            int sftpPort = 22;
            if (StringUtils.isNotBlank(port)) {
                sftpPort = Integer.parseInt(port);
            }

            JSch jsch = new JSch();
            Session jschSession = jsch.getSession(username, host, sftpPort);
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            jschSession.setConfig(config);
            jschSession.setPassword(password);
            jschSession.connect();

            ChannelSftp sftp = (ChannelSftp) jschSession.openChannel("sftp");
            sftp.connect();

            return sftp;
        } catch (JSchException e) {
            throw new IOException(e.getMessage(), e);
        }
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

    public static void main(String[] args) throws Exception {
        Map<String, String> options = Maps.newHashMap();
        options.put("host", "10.10.9.11");
        options.put("port", "22");
        options.put("username", "sftpuser");
        options.put("password", "dz@2021");

        ChannelSftp channelSftp = setupJsch(options);

        boolean result = checkFileExists(channelSftp, "/upload/demo.csv");
        System.out.println(result);

        rename(channelSftp, "/upload/demo.csv", "/upload/demo1.csv");

        channelSftp.getSession().disconnect();
        System.out.println("==");
    }
}
