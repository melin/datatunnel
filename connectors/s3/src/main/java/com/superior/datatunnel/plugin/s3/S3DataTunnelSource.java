package com.superior.datatunnel.plugin.s3;

import com.amazonaws.SDKGlobalConfiguration;
import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
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

        System.setProperty(S3Configs.awsServicesEnableV4, "true");
        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");

        SparkSession sparkSession = context.getSparkSession();
        sparkSession.sparkContext().hadoopConfiguration().set(S3Configs.accessKey, sourceOption.getAccessKey());
        sparkSession.sparkContext().hadoopConfiguration().set(S3Configs.secretKey, sourceOption.getSecretKey());
        sparkSession.sparkContext().hadoopConfiguration().set(S3Configs.s3aClientImpl, sourceOption.getS3aClientImpl());
        sparkSession.sparkContext().hadoopConfiguration().set(S3Configs.sslEnabled, "true");
        sparkSession.sparkContext().hadoopConfiguration().set(S3Configs.endPoint, sourceOption.getEndpoint());
        sparkSession.sparkContext().hadoopConfiguration().set(S3Configs.pathStyleAccess, "true");

        return sparkSession.read().format(sourceOption.getFormat().name().toLowerCase())
                .option("header", "true")
                .option("inferSchema", "true")
                .load(sourceOption.getFilePath());
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return S3DataTunnelSourceOption.class;
    }
}
