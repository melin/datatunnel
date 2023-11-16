package com.superior.datatunnel.plugin.redshift;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.io.IOException;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class RedshiftDataTunnelSource implements DataTunnelSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedshiftDataTunnelSource.class);

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        SparkSession sparkSession = context.getSparkSession();
        RedshiftDataTunnelSourceOption option = (RedshiftDataTunnelSourceOption) context.getSourceOption();

        String jdbcURL = "jdbc:redshift://" + option.getHost() + ":" + option.getPort() + "/" + option.getDatabaseName();

        String accessKeyId = option.getAccessKeyId();
        String secretAccessKey = option.getSecretAccessKey();
        String region = option.getRegion();
        String redshiftRoleArn = option.getRedshiftRoleArn();
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", accessKeyId);
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", secretAccessKey);
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint.region", region);

        DataFrameReader reader = sparkSession
                .read()
                .format("io.github.spark_redshift_community.spark.redshift")
                .options(option.getProperties())
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
            reader.option("temporary_aws_access_key_id", credentials.accessKeyId())
                    .option("temporary_aws_secret_access_key", credentials.secretAccessKey())
                    .option("temporary_aws_session_token", credentials.sessionToken());
        }

        if (StringUtils.isNotBlank(option.getQuery())) {
            reader.option("query", option.getQuery());
        } else {
            if (StringUtils.isBlank(option.getSchemaName())) {
                throw new DataTunnelException("schemaName can not blank");
            }
            if (StringUtils.isBlank(option.getTableName())) {
                throw new DataTunnelException("tableName can not blank");
            }

            reader.option("dbtable", option.getSchemaName() + "." + option.getTableName());
        }

        return reader.load();
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return RedshiftDataTunnelSourceOption.class;
    }
}
