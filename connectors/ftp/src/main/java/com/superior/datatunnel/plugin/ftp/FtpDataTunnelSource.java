package com.superior.datatunnel.plugin.ftp;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import com.superior.datatunnel.common.enums.FileFormat;
import com.superior.datatunnel.hadoop.fs.ftp.FTPFileSystem;
import com.superior.datatunnel.hadoop.fs.sftp.SFTPFileSystem;
import com.superior.datatunnel.plugin.ftp.enums.AuthType;
import com.superior.datatunnel.plugin.ftp.enums.FtpProtocol;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class FtpDataTunnelSource implements DataTunnelSource {

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        FtpDataTunnelSourceOption sourceOption = (FtpDataTunnelSourceOption) context.getSourceOption();

        SparkSession sparkSession = context.getSparkSession();
        Configuration hadoopConf = sparkSession.sparkContext().hadoopConfiguration();
        sourceOption.getProperties().forEach((key, value) -> {
            if (key.startsWith("fs.")) {
                hadoopConf.set(key, value);
            }
        });

        String host = sourceOption.getHost();
        String port = String.valueOf(sourceOption.getPort());
        String prefix = "fs.ftp.";
        if (sourceOption.getProtocol() == FtpProtocol.SFTP) {
            prefix =  "fs.sftp.";
        }

        hadoopConf.set(prefix + "host", host);
        hadoopConf.set(prefix + "host.port", port);
        hadoopConf.set(prefix + "user." + host, sourceOption.getUsername());

        if (FtpProtocol.SFTP == sourceOption.getProtocol()) {
            hadoopConf.set(prefix + "impl", SFTPFileSystem.class.getName());

            if (AuthType.SSHKEY == sourceOption.getAuthType()) {
                if (StringUtils.isBlank(sourceOption.getSshKeyFile())) {
                    throw new DataTunnelException("sshkey 认证方式，sshKeyFile 不能为空");
                }

                hadoopConf.set(prefix + "key.file." + host + "." + sourceOption.getUsername(), sourceOption.getSshKeyFile());
                if (StringUtils.isNotBlank(sourceOption.getSshPassphrase())) {
                    hadoopConf.set(prefix + "key.passphrase." + host + "." + sourceOption.getUsername(), sourceOption.getSshPassphrase());
                }
            } else {
                if (StringUtils.isBlank(sourceOption.getPassword())) {
                    throw new DataTunnelException("password can not blank");
                }
                hadoopConf.set(prefix + "password." + host + "." + sourceOption.getUsername(), sourceOption.getPassword());
            }
        } else {
            hadoopConf.set(prefix + "impl", FTPFileSystem.class.getName());

            if (StringUtils.isBlank(sourceOption.getPassword())) {
                throw new DataTunnelException("password can not blank");
            }
            hadoopConf.set(prefix + "password." + host + "." + sourceOption.getUsername(), sourceOption.getPassword());
        }
        hadoopConf.set(prefix + "impl.disable.cache", "false");

        /*if (sourceOption.getConnectionMode() != null) {
            hadoopConf.set(FS_FTP_DATA_CONNECTION_MODE, "PASSIVE_" + sourceOption.getConnectionMode().name() + "_DATA_CONNECTION_MODE");
        }

        if (sourceOption.getTransferMode() != null) {
            hadoopConf.set(FS_FTP_TRANSFER_MODE, sourceOption.getTransferMode().name() + "_TRANSFER_MODE");
        }*/

        String format = sourceOption.getFormat().name().toLowerCase();
        if (FileFormat.EXCEL == sourceOption.getFormat()) {
            format = "com.crealytics.spark.excel";
        }
        DataFrameReader reader = sparkSession.read().format(format);
        reader.options(sourceOption.getProperties());
        if ("csv".equalsIgnoreCase(format)) {
            reader.option("sep", sourceOption.getSep());
            reader.option("encoding", sourceOption.getEncoding());
            reader.option("header", sourceOption.isHeader());
        }
        return reader.load(sourceOption.getFilePath());
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return FtpDataTunnelSourceOption.class;
    }
}
