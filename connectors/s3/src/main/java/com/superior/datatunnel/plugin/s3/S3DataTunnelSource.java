package com.superior.datatunnel.plugin.s3;

import com.amazonaws.SDKGlobalConfiguration;
import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import com.superior.datatunnel.common.enums.FileFormat;
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
public class S3DataTunnelSource implements DataTunnelSource {

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        S3DataTunnelSourceOption sourceOption = (S3DataTunnelSourceOption) context.getSourceOption();

        if (StringUtils.isBlank(sourceOption.getRegion()) && StringUtils.isBlank(sourceOption.getEndpoint())) {
            throw new DataTunnelException("region 和 endpoint 不能同时为空");
        }

        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
        System.setProperty(SDKGlobalConfiguration.DEFAULT_METRICS_SYSTEM_PROPERTY, "false");

        SparkSession sparkSession = context.getSparkSession();
        Configuration hadoopConf = sparkSession.sparkContext().hadoopConfiguration();
        sourceOption.getProperties().forEach((key, value) -> {
            if (key.startsWith("fs.")) {
                hadoopConf.set(key, value);
            }
        });

        hadoopConf.set(S3Configs.ACCESS_KEY, sourceOption.getAccessKey());
        hadoopConf.set(S3Configs.SECRET_KEY, sourceOption.getSecretKey());
        if (StringUtils.isNotBlank(sourceOption.getRegion())) {
            hadoopConf.set(S3Configs.REGION, sourceOption.getRegion());
        }
        if (StringUtils.isNotBlank(sourceOption.getEndpoint())) {
            hadoopConf.set(S3Configs.END_POINT, sourceOption.getEndpoint());
        }

        if (!sourceOption.getProperties().containsKey(S3Configs.S3A_CLIENT_IMPL)) {
            hadoopConf.set(S3Configs.S3A_CLIENT_IMPL, sourceOption.getS3aClientImpl());
        }

        String format = sourceOption.getFormat().name().toLowerCase();
        if (FileFormat.EXCEL == sourceOption.getFormat()) {
            format = "com.crealytics.spark.excel";
        }
        DataFrameReader reader = sparkSession.read().format(format);

        sourceOption.getProperties().forEach((key, value) -> {
            if (!key.startsWith("fs.")) {
                reader.option(key, value);
            }
        });

        return reader.load(sourceOption.getFilePath());
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return S3DataTunnelSourceOption.class;
    }
}
