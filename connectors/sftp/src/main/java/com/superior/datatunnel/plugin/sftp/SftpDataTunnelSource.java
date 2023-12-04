package com.superior.datatunnel.plugin.sftp;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import com.superior.datatunnel.common.enums.FileFormat;
import com.superior.datatunnel.hadoop.fs.sftp.SFTPFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

import static com.superior.datatunnel.hadoop.fs.sftp.SFTPFileSystem.*;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class SftpDataTunnelSource implements DataTunnelSource {

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        SftpDataTunnelSourceOption sourceOption = (SftpDataTunnelSourceOption) context.getSourceOption();

        SparkSession sparkSession = context.getSparkSession();
        Configuration hadoopConf = sparkSession.sparkContext().hadoopConfiguration();
        sourceOption.getProperties().forEach((key, value) -> {
            if (key.startsWith("fs.")) {
                hadoopConf.set(key, value);
            }
        });
        String host = sourceOption.getHost();
        String port = String.valueOf(sourceOption.getPort());

        hadoopConf.set(FS_SFTP_HOST, host);
        hadoopConf.set(FS_SFTP_HOST_PORT, port);
        hadoopConf.set(FS_SFTP_USER_PREFIX + host, sourceOption.getUsername());
        hadoopConf.set(FS_SFTP_PASSWORD_PREFIX + host, sourceOption.getPassword());
        hadoopConf.set(FS_SFTP_KEYFILE, sourceOption.getKeyFilePath());

        hadoopConf.set("fs.sftp.impl", SFTPFileSystem.class.getName());
        hadoopConf.set("fs.sftp.impl.disable.cache", "false");

        String format = sourceOption.getFormat().name().toLowerCase();
        if (FileFormat.EXCEL == sourceOption.getFormat()) {
            format = "com.crealytics.spark.excel";
        }
        DataFrameReader reader = sparkSession.read().format(format);
        sourceOption.getProperties().forEach(reader::option);
        reader.option("wholetext", "true");

        return reader.load(sourceOption.getFilePath());
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return SftpDataTunnelSourceOption.class;
    }
}
