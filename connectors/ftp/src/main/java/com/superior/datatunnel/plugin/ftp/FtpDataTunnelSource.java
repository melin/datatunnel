package com.superior.datatunnel.plugin.ftp;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import com.superior.datatunnel.common.enums.FileFormat;
import com.superior.datatunnel.plugin.ftp.fs.FTPFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

import static com.superior.datatunnel.plugin.ftp.fs.FTPFileSystem.*;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class FtpDataTunnelSource implements DataTunnelSource {

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        FtpDataTunnelSourceOption sourceOption = (FtpDataTunnelSourceOption) context.getSourceOption();

        SparkSession sparkSession = context.getSparkSession();
        Configuration hadoopConf = sparkSession.sparkContext().hadoopConfiguration();
        hadoopConf.set(FS_FTP_HOST, sourceOption.getHost());
        hadoopConf.set(FS_FTP_PORT, String.valueOf(sourceOption.getPort()));
        hadoopConf.set(FS_FTP_USERNAME, sourceOption.getUsername());
        hadoopConf.set(FS_FTP_PASSWORD, sourceOption.getPassword());
        hadoopConf.set(FS_FTP_TIMEOUT, String.valueOf(sourceOption.getKeepAliveTimeout()));

        hadoopConf.set("fs.ftp.impl", FTPFileSystem.class.getName());
        hadoopConf.set("fs.ftp.impl.disable.cache", "false");

        if (sourceOption.getConnectionMode() != null) {
            hadoopConf.set(FS_FTP_DATA_CONNECTION_MODE, "PASSIVE_" + sourceOption.getConnectionMode().name() + "_DATA_CONNECTION_MODE");
        }

        if (sourceOption.getTransferMode() != null) {
            hadoopConf.set(FS_FTP_TRANSFER_MODE, sourceOption.getTransferMode().name() + "_TRANSFER_MODE");
        }

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
        return FtpDataTunnelSourceOption.class;
    }
}
