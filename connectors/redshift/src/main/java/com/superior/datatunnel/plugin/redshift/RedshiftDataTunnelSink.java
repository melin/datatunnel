package com.superior.datatunnel.plugin.redshift;

import com.gitee.melin.bee.util.RandomUniqueId;
import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.common.enums.WriteMode;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

public class RedshiftDataTunnelSink implements DataTunnelSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedshiftDataTunnelSink.class);

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        SparkSession sparkSession = context.getSparkSession();
        RedshiftDataTunnelSinkOption option = (RedshiftDataTunnelSinkOption) context.getSinkOption();

        final WriteMode writeMode = option.getWriteMode();
        if (writeMode == WriteMode.BULKINSERT) {
            throw new DataTunnelException("reshift writer not support Bulk insert");
        }

        String[] preActions = option.getPreActions();
        String[] postActions = option.getPostActions();
        String[] upsertKeyColumns = option.getUpsertKeyColumns();
        if (preActions == null) {
            preActions = new String[]{};
        }
        if (postActions == null) {
            postActions = new String[]{};
        }

        String schemaName = option.getSchemaName();
        String tableName = option.getTableName();
        String dbtable;
        if (writeMode == WriteMode.UPSERT) {
            if (upsertKeyColumns == null) {
                throw new DataTunnelException("UPSERT mode, upsertKeyColumns can not blank");
            }

            // io.github.spark_redshift_community.spark.redshift.TableName
            String oldDbtable = "\"" + schemaName + "\".\"" + option.getTableName() + "\"";

            // preactions
            tableName = tableName + "_" + RandomUniqueId.getNewLowerString(12);
            dbtable = "\"" + schemaName + "\".\"" + tableName + "\"";
            // RedshiftWriter 自动创建不存在的表

            String where = Arrays.stream(upsertKeyColumns)
                    .map(col -> dbtable + "." + col + " = " + oldDbtable + "." + col)
                    .collect(Collectors.joining(" and "));

            // postactions
            postActions = ArrayUtils.add(postActions, "BEGIN;");
            String sql = "DELETE FROM " + oldDbtable + " USING " + dbtable + " WHERE " + where + ";";
            postActions = ArrayUtils.add(postActions, sql);
            sql = "INSERT INTO " + oldDbtable + " SELECT * FROM " + dbtable + ";";
            postActions = ArrayUtils.add(postActions, sql);
            sql = "DROP TABLE IF EXISTS " + dbtable + ";";
            postActions = ArrayUtils.add(postActions, sql);
            postActions = ArrayUtils.add(postActions, "END;");
        } else if (writeMode == WriteMode.OVERWRITE) {
            dbtable = "\"" + schemaName + "\".\"" + tableName + "\"";
            // preactions
            String sql = "TRUNCATE TABLE " + dbtable + ";";
            preActions = ArrayUtils.add(preActions, sql);
        } else {
            dbtable = "\"" + schemaName + "\".\"" + tableName + "\"";
        }

        String jdbcUrl = option.getJdbcUrl();
        if (StringUtils.isBlank(jdbcUrl)) {
            if (StringUtils.isNotBlank(option.getHost())
                    && option.getPort() != null && StringUtils.isNotBlank(option.getDatabaseName())) {
                jdbcUrl = "jdbc:redshift://" + option.getHost() + ":" + option.getPort() + "/" + option.getDatabaseName();
            } else {
                throw new IllegalArgumentException("Redshift 不正确，添加jdbcUrl 或者 host & port & databaseName");
            }
        }

        String accessKeyId = option.getAccessKeyId();
        String secretAccessKey = option.getSecretAccessKey();
        String region = option.getRegion();
        String iamRole = option.getIamRole();
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", accessKeyId);
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", secretAccessKey);
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint.region", region);

        DataFrameWriter dataFrameWriter = dataset.write()
                .format("io.github.spark_redshift_community.spark.redshift")
                .options(option.getProperties())
                .option("url", jdbcUrl)
                .option("user", option.getUsername())
                .option("password", option.getPassword())
                .option("tempdir_region", option.getRegion())
                .option("tempdir", option.getTempdir());

        if (!option.getParams().containsKey("aws_iam_role")) {
            if (StringUtils.isBlank(iamRole)) {
                throw new DataTunnelException("iamRole can not blank");
            }
            Credentials credentials = Utils.queryCredentials(accessKeyId, secretAccessKey, region, iamRole);
            dataFrameWriter.option("temporary_aws_access_key_id", credentials.accessKeyId())
                    .option("temporary_aws_secret_access_key", credentials.secretAccessKey())
                    .option("temporary_aws_session_token", credentials.sessionToken());
        }

        if (preActions != null && preActions.length > 0) {
            dataFrameWriter.option("preactions", StringUtils.join(preActions, ";"));
        }

        if (postActions != null && postActions.length > 0) {
            dataFrameWriter.option("postactions", StringUtils.join(postActions, ";"));
        }

        dataFrameWriter.option("dbtable", dbtable);
        dataFrameWriter.mode(SaveMode.Append).save();
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return RedshiftDataTunnelSinkOption.class;
    }
}
