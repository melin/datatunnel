package com.superior.datatunnel.plugin.redshift;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.io.IOException;

public class RedshiftDataTunnelSink implements DataTunnelSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedshiftDataTunnelSink.class);

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        SparkSession sparkSession = context.getSparkSession();
        RedshiftDataTunnelSinkOption option = (RedshiftDataTunnelSinkOption) context.getSinkOption();

        String jdbcURL = "jdbc:redshift://" + option.getHost() + ":" + option.getPort() + "/" + option.getDatabaseName();

        String accessKeyId = option.getAccessKeyId();
        String secretAccessKey = option.getSecretAccessKey();
        String region = option.getRegion();
        String redshiftRoleArn = option.getRedshiftRoleArn();
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", accessKeyId);
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", secretAccessKey);
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint.region", region);

        DataFrameWriter dataFrameWriter = dataset.write()
                .format("io.github.spark_redshift_community.spark.redshift")
                .options(option.getParams())
                .option("url", jdbcURL)
                .option("user", option.getUsername())
                .option("password", option.getPassword())
                .option("tempdir_region", option.getRegion())
                .option("tempdir", option.getTempdir());

        if (!option.getParams().containsKey("aws_iam_role")) {
            if (StringUtils.isBlank(redshiftRoleArn)) {
                throw new DataTunnelException("redshiftRoleArn can not blank");
            }
            Credentials credentials = Utils.queryCredentials(accessKeyId, secretAccessKey, region, redshiftRoleArn);
            dataFrameWriter.option("temporary_aws_access_key_id", credentials.accessKeyId())
                    .option("temporary_aws_secret_access_key", credentials.secretAccessKey())
                    .option("temporary_aws_session_token", credentials.sessionToken());
        }

        if (StringUtils.isBlank(option.getSchemaName())) {
            throw new DataTunnelException("schemaName can not blank");
        }
        if (StringUtils.isBlank(option.getTableName())) {
            throw new DataTunnelException("tableName can not blank");
        }

        dataFrameWriter.option("dbtable", option.getSchemaName() + "." + option.getTableName());
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return RedshiftDataTunnelSinkOption.class;
    }
}
