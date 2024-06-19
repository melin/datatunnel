package com.superior.datatunnel.plugin.s3;

import com.amazonaws.SDKGlobalConfiguration;
import com.superior.datatunnel.api.DataSourceType;
import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import com.superior.datatunnel.common.enums.FileFormat;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class S3DataTunnelSource implements DataTunnelSource {

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        S3DataTunnelSourceOption sourceOption = (S3DataTunnelSourceOption) context.getSourceOption();

        // 在aws 运行环境，不需要指定 reigon 和 endpoint, 如果本地调试需要指定 region。
        if (context.getSourceType() != DataSourceType.S3) {
            if (StringUtils.isBlank(sourceOption.getRegion()) && StringUtils.isBlank(sourceOption.getEndpoint())) {
                throw new DataTunnelException("region 和 endpoint 不能同时为空");
            }
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

        setObjConfig(context.getSourceType(), sourceOption, hadoopConf);

        if ("csv".equalsIgnoreCase(format)) {
            reader.option("sep", sourceOption.getSep());
            reader.option("encoding", sourceOption.getEncoding());
            reader.option("header", sourceOption.isHeader());
        }
        if (StringUtils.isNotBlank(sourceOption.getLineSep())) {
            reader.option("lineSep", sourceOption.getLineSep());
        }

        return reader.load(sourceOption.getPaths());
    }

    private void setObjConfig(
            DataSourceType sourceType, S3DataTunnelSourceOption sinkOption, Configuration hadoopConf) {
        if (sourceType == DataSourceType.S3 || sourceType == DataSourceType.MINIO) {
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
        } else if (sourceType == DataSourceType.OSS) {
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
            throw new IllegalArgumentException(this.getClass().getSimpleName() + " not support type: " + sourceType);
        }
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return S3DataTunnelSourceOption.class;
    }
}
