package com.superior.datatunnel.plugin.ftp;

import com.superior.datatunnel.api.*;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.common.enums.FileFormat;
import com.superior.datatunnel.hadoop.fs.ftp.FTPFileSystem;
import com.superior.datatunnel.hadoop.fs.sftp.SFTPFileSystem;
import com.superior.datatunnel.plugin.ftp.enums.AuthType;
import com.superior.datatunnel.plugin.ftp.enums.FtpProtocol;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class FtpDataTunnelSink implements DataTunnelSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(FtpDataTunnelSink.class);

    private void validateOptions(DataTunnelContext context) {
    }

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        validateOptions(context);

        FtpDataTunnelSinkOption sinkOption = (FtpDataTunnelSinkOption) context.getSinkOption();

        SparkSession sparkSession = context.getSparkSession();
        Configuration hadoopConf = sparkSession.sparkContext().hadoopConfiguration();
        sinkOption.getProperties().forEach((key, value) -> {
            if (key.startsWith("fs.")) {
                hadoopConf.set(key, value);
            }
        });

        String host = sinkOption.getHost();
        String port = String.valueOf(sinkOption.getPort());
        String prefix = "fs.ftp.";
        if (sinkOption.getProtocol() == FtpProtocol.SFTP) {
            prefix =  "fs.sftp.";
        }

        hadoopConf.set(prefix + "host", host);
        hadoopConf.set(prefix + "host.port", port);
        hadoopConf.set(prefix + "user." + host, sinkOption.getUsername());

        if (FtpProtocol.SFTP == sinkOption.getProtocol()) {
            hadoopConf.set(prefix + "impl", SFTPFileSystem.class.getName());
            if (AuthType.SSHKEY == sinkOption.getAuthType()) {
                if (StringUtils.isBlank(sinkOption.getSshKeyFile())) {
                    throw new DataTunnelException("sshkey 认证方式，sshKeyFile 不能为空");
                }

                hadoopConf.set(prefix + "key.file." + host + "." + sinkOption.getUsername(), sinkOption.getSshKeyFile());
                if (StringUtils.isNotBlank(sinkOption.getSshPassphrase())) {
                    hadoopConf.set(prefix + "key.passphrase." + host + "." + sinkOption.getUsername(), sinkOption.getSshPassphrase());
                }
            } else {
                if (StringUtils.isBlank(sinkOption.getPassword())) {
                    throw new DataTunnelException("password can not blank");
                }
                hadoopConf.set(prefix + "password." + host + "." + sinkOption.getUsername(), sinkOption.getPassword());
            }
        } else {
            hadoopConf.set(prefix + "impl", FTPFileSystem.class.getName());

            if (StringUtils.isBlank(sinkOption.getPassword())) {
                throw new DataTunnelException("password can not blank");
            }
            hadoopConf.set(prefix + "password." + host + "." + sinkOption.getUsername(), sinkOption.getPassword());
        }
        hadoopConf.set(prefix + "impl.disable.cache", "false");

        /*if (sinkOption.getConnectionMode() != null) {
            hadoopConf.set(FS_FTP_DATA_CONNECTION_MODE, "PASSIVE_" + sinkOption.getConnectionMode().name() + "_DATA_CONNECTION_MODE");
        }

        if (sinkOption.getTransferMode() != null) {
            hadoopConf.set(FS_FTP_TRANSFER_MODE, sinkOption.getTransferMode().name() + "_TRANSFER_MODE");
        }*/

        String format = sinkOption.getFormat().name().toLowerCase();
        if (FileFormat.EXCEL == sinkOption.getFormat()) {
            format = "com.crealytics.spark.excel";
        }

        DataFrameWriter writer = dataset.write()
                .mode(sinkOption.getWriteMode().name().toLowerCase())
                .format(format);

        writer.options(sinkOption.getProperties());
        if ("csv".equalsIgnoreCase(format)) {
            writer.option("sep", sinkOption.getSep());
            writer.option("encoding", sinkOption.getEncoding());
            writer.option("header", sinkOption.isHeader());
        }
        writer.option("compression", sinkOption.getCompression());

        String path = sinkOption.getPath();
        if (FtpProtocol.SFTP == sinkOption.getProtocol()) {
            path = "sftp:////" + path;
        } else {
            path = "ftp:////" + path;
        }
        writer.save(path);
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return FtpDataTunnelSinkOption.class;
    }
}
