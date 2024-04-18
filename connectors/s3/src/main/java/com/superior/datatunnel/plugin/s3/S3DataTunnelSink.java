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

        if (context.getSinkType() == DataSourceType.S3) {
            System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
            System.setProperty(SDKGlobalConfiguration.DEFAULT_METRICS_SYSTEM_PROPERTY, "false");
        }

        SparkSession sparkSession = context.getSparkSession();
        Configuration hadoopConf = sparkSession.sparkContext().hadoopConfiguration();
        sinkOption.getProperties().forEach((key, value) -> {
            if (key.startsWith("fs.")) {
                hadoopConf.set(key, value);
            }
        });

        String format = sinkOption.getFormat().name().toLowerCase();
        if (FileFormat.EXCEL == sinkOption.getFormat()) {
            format = "com.crealytics.spark.excel";
        }

        DataFrameWriter writer = dataset.write()
                .format(format)
                .mode(sinkOption.getWriteMode().name().toLowerCase());

        sinkOption.getProperties().forEach((key, value) -> {
            if (!key.startsWith("fs.")) {
                writer.option(key, value);
            }
        });

        setObjConfig(context.getSinkType(), sinkOption, hadoopConf);

        if ("csv".equalsIgnoreCase(format)) {
            writer.option("sep", sinkOption.getSep());
            writer.option("encoding", sinkOption.getEncoding());
            writer.option("header", sinkOption.isHeader());
        }

        writer.option("compression", sinkOption.getCompression());
        writer.save(sinkOption.getPath());
    }

    private void setObjConfig(DataSourceType sinkType, S3DataTunnelSinkOption sinkOption, Configuration hadoopConf) {
        if (sinkType == DataSourceType.S3 || sinkType == DataSourceType.MINIO) {
            if (StringUtils.isNotBlank(sinkOption.getAccessKey())) {
                hadoopConf.set(S3Configs.AWS_ACCESS_KEY, sinkOption.getAccessKey());
            }
            if (StringUtils.isNotBlank(sinkOption.getSecretKey())) {
                hadoopConf.set(S3Configs.AWS_SECRET_KEY, sinkOption.getSecretKey());
            }
            if (StringUtils.isNotBlank(sinkOption.getRegion())) {
                hadoopConf.set(S3Configs.AWS_REGION, sinkOption.getRegion());
            }
            if (StringUtils.isNotBlank(sinkOption.getEndpoint())) {
                hadoopConf.set(S3Configs.AWS_ENDPOINT, sinkOption.getEndpoint());
            }
            if (StringUtils.isNotBlank(sinkOption.getFsClientImpl())) {
                hadoopConf.set(S3Configs.AWS_S3A_CLIENT_IMPL, sinkOption.getFsClientImpl());
            }
        } else if (sinkType == DataSourceType.OSS) {
            if (StringUtils.isNotBlank(sinkOption.getAccessKey())) {
                hadoopConf.set(S3Configs.OSS_ACCESS_KEY, sinkOption.getAccessKey());
            }
            if (StringUtils.isNotBlank(sinkOption.getSecretKey())) {
                hadoopConf.set(S3Configs.OSS_SECRET_KEY, sinkOption.getSecretKey());
            }
            if (StringUtils.isNotBlank(sinkOption.getEndpoint())) {
                hadoopConf.set(S3Configs.OSS_ENDPOINT, sinkOption.getEndpoint());
            }
            if (StringUtils.isNotBlank(sinkOption.getFsClientImpl())) {
                hadoopConf.set(S3Configs.OSS_S3A_CLIENT_IMPL, sinkOption.getFsClientImpl());
            } else {
                hadoopConf.set(S3Configs.OSS_S3A_CLIENT_IMPL, "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem");
            }
        } else {
            throw new IllegalArgumentException(this.getClass().getSimpleName() + " not support type: " + sinkType);
        }
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return S3DataTunnelSinkOption.class;
    }
}
