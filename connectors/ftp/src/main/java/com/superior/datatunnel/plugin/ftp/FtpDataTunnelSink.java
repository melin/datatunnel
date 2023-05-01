package com.superior.datatunnel.plugin.ftp;

import com.superior.datatunnel.api.*;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.common.enums.FileFormat;
import com.superior.datatunnel.plugin.ftp.fs.FTPFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.superior.datatunnel.plugin.ftp.fs.FTPFileSystem.*;

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
        hadoopConf.set(FS_FTP_HOST, sinkOption.getHost());
        hadoopConf.set(FS_FTP_PORT, String.valueOf(sinkOption.getPort()));
        hadoopConf.set(FS_FTP_USERNAME, sinkOption.getUsername());
        hadoopConf.set(FS_FTP_PASSWORD, sinkOption.getPassword());
        hadoopConf.set(FS_FTP_TIMEOUT, String.valueOf(sinkOption.getKeepAliveTimeout()));

        hadoopConf.set("fs.ftp.impl", FTPFileSystem.class.getName());
        hadoopConf.set("fs.ftp.impl.disable.cache", "false");

        if (sinkOption.getConnectionMode() != null) {
            hadoopConf.set(FS_FTP_DATA_CONNECTION_MODE, "PASSIVE_" + sinkOption.getConnectionMode().name() + "_DATA_CONNECTION_MODE");
        }

        if (sinkOption.getTransferMode() != null) {
            hadoopConf.set(FS_FTP_TRANSFER_MODE, sinkOption.getTransferMode().name() + "_TRANSFER_MODE");
        }

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
        return FtpDataTunnelSinkOption.class;
    }
}
