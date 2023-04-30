package com.superior.datatunnel.plugin.s3;

import com.amazonaws.SDKGlobalConfiguration;
import com.superior.datatunnel.api.*;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.common.enums.FileFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.superior.datatunnel.api.DataSourceType.HDFS;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class S3DataTunnelSink implements DataTunnelSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3DataTunnelSink.class);

    private void validateOptions(DataTunnelContext context) {
        DataSourceType dsType = context.getSourceOption().getDataSourceType();
        if (HDFS == dsType) {
            throw new DataTunnelException("只支持从hdfs读取文件写入sftp");
        }
    }

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        validateOptions(context);

        S3DataTunnelSinkOption sinkOption = (S3DataTunnelSinkOption) context.getSinkOption();

        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
        System.setProperty(SDKGlobalConfiguration.DEFAULT_METRICS_SYSTEM_PROPERTY, "false");

        SparkSession sparkSession = context.getSparkSession();
        Configuration hadoopConf = sparkSession.sparkContext().hadoopConfiguration();
        hadoopConf.set(S3Configs.ACCESS_KEY, sinkOption.getAccessKey());
        hadoopConf.set(S3Configs.SECRET_KEY, sinkOption.getSecretKey());
        hadoopConf.set(S3Configs.S3A_CLIENT_IMPL, sinkOption.getS3aClientImpl());
        hadoopConf.set(S3Configs.SSL_ENABLED, String.valueOf(sinkOption.isSslEnabled()));
        hadoopConf.set(S3Configs.END_POINT, sinkOption.getEndpoint());
        hadoopConf.set(S3Configs.PATH_STYLE_ACCESS, String.valueOf(sinkOption.isPathStyleAccess()));
        hadoopConf.set(S3Configs.CONNECTION_TIMEOUT, String.valueOf(sinkOption.getConnectionTimeout()));

        String format = sinkOption.getFormat().name().toLowerCase();
        if (FileFormat.EXCEL == sinkOption.getFormat()) {
            format = "com.crealytics.spark.excel";
        }

        DataFrameWriter writer = dataset.write()
                .format(format)
                .mode(sinkOption.getSaveMode().name().toLowerCase());

        sinkOption.getProperties().forEach(writer::option);
        writer.save(sinkOption.getFilePath());
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return S3DataTunnelSinkOption.class;
    }
}
