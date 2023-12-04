package com.superior.datatunnel.plugin.s3;

import com.amazonaws.SDKGlobalConfiguration;
import com.superior.datatunnel.api.*;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.common.enums.FileFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class S3DataTunnelSink implements DataTunnelSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3DataTunnelSink.class);

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        S3DataTunnelSinkOption sinkOption = (S3DataTunnelSinkOption) context.getSinkOption();

        if (StringUtils.isBlank(sinkOption.getRegion()) && StringUtils.isBlank(sinkOption.getEndpoint())) {
            throw new DataTunnelException("region 和 endpoint 不能同时为空");
        }

        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
        System.setProperty(SDKGlobalConfiguration.DEFAULT_METRICS_SYSTEM_PROPERTY, "false");

        SparkSession sparkSession = context.getSparkSession();
        Configuration hadoopConf = sparkSession.sparkContext().hadoopConfiguration();
        sinkOption.getProperties().forEach((key, value) -> {
            if (key.startsWith("fs.")) {
                hadoopConf.set(key, value);
            }
        });

        hadoopConf.set(S3Configs.ACCESS_KEY, sinkOption.getAccessKey());
        hadoopConf.set(S3Configs.SECRET_KEY, sinkOption.getSecretKey());
        if (StringUtils.isNotBlank(sinkOption.getRegion())) {
            hadoopConf.set(S3Configs.REGION, sinkOption.getRegion());
        }
        if (StringUtils.isNotBlank(sinkOption.getEndpoint())) {
            hadoopConf.set(S3Configs.END_POINT, sinkOption.getEndpoint());
        }

        if (!sinkOption.getProperties().containsKey(S3Configs.S3A_CLIENT_IMPL)) {
            hadoopConf.set(S3Configs.S3A_CLIENT_IMPL, sinkOption.getS3aClientImpl());
        }

        String format = sinkOption.getFormat().name().toLowerCase();
        if (FileFormat.EXCEL == sinkOption.getFormat()) {
            format = "com.crealytics.spark.excel";
        }

        DataFrameWriter writer = dataset.write()
                .format(format)
                .mode(sinkOption.getSaveMode().name().toLowerCase());

        sinkOption.getProperties().forEach((key, value) -> {
            if (!key.startsWith("fs.")) {
                writer.option(key, value);
            }
        });

        writer.save(sinkOption.getFilePath());
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return S3DataTunnelSinkOption.class;
    }
}
