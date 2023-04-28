package com.superior.datatunnel.plugin.sftp;

import com.superior.datatunnel.api.*;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.common.enums.FileFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ftp.FTPFileSystem;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.superior.datatunnel.plugin.sftp.fs.SFTPFileSystem.*;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class SftpDataTunnelSink implements DataTunnelSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(SftpDataTunnelSink.class);

    private void validateOptions(DataTunnelContext context) {
    }

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        validateOptions(context);

        SftpDataTunnelSinkOption sinkOption = (SftpDataTunnelSinkOption) context.getSinkOption();

        SparkSession sparkSession = context.getSparkSession();
        Configuration hadoopConf = sparkSession.sparkContext().hadoopConfiguration();
        hadoopConf.set(FS_SFTP_HOST, sinkOption.getHost());
        hadoopConf.set(FS_SFTP_PORT, String.valueOf(sinkOption.getPort()));
        hadoopConf.set(FS_SFTP_USERNAME, sinkOption.getUsername());
        hadoopConf.set(FS_SFTP_PASSWORD, sinkOption.getPassword());
        hadoopConf.set(FS_SFTP_KEYFILE, sinkOption.getKeyFilePath());
        hadoopConf.set(FS_SFTP_PASSPHRASE, sinkOption.getPassPhrase());

        hadoopConf.set("fs.sftp.impl", FTPFileSystem.class.getName());
        hadoopConf.set("fs.sftp.impl.disable.cache", "false");

        String format = sinkOption.getFormat().name().toLowerCase();
        if (FileFormat.EXCEL == sinkOption.getFormat()) {
            format = "com.crealytics.spark.excel";
        }

        DataFrameWriter writer = dataset.write().format(format);
        sinkOption.getProperties().forEach(writer::option);
        writer.save(sinkOption.getFilePath());
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return SftpDataTunnelSinkOption.class;
    }
}
