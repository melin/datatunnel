package com.superior.datatunnel.plugin.redis;

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
public class RedisDataTunnelSource implements DataTunnelSource {

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        RedisDataTunnelSourceOption sourceOption = (RedisDataTunnelSourceOption) context.getSourceOption();

        System.setProperty(RedisConfigs.awsServicesEnableV4, "true");

        SparkSession sparkSession = context.getSparkSession();
        sparkSession.conf().set(RedisConfigs.accessKey, sourceOption.getAccessKey());
        sparkSession.conf().set(RedisConfigs.secretKey, sourceOption.getSecretKey());
        sparkSession.conf().set(RedisConfigs.s3aClientImpl, sourceOption.getS3aClientImpl());
        sparkSession.conf().set(RedisConfigs.sslEnabled, sourceOption.isSslEnabled());
        sparkSession.conf().set(RedisConfigs.endPoint, sourceOption.getEndpoint());
        sparkSession.conf().set(RedisConfigs.pathStyleAccess, sourceOption.isPathStyleAccess());

        /* val dataframe = conf.objectFormat match {
            case S3Constants.csvFileFormat =>
                sparkSession.read.format("csv")
                        .option(S3Constants.delimiter, conf.delimiter)
                        .option(S3Constants.inferschema, conf.inferSchema)
                        .option(S3Constants.header, conf.header)
                        .load(conf.objectPath)
            case S3Constants.jsonFileformat =>
                sparkSession.read.json(conf.objectPath)
            case S3Constants.parquetFileFormat =>
                sparkSession.read.parquet((conf.objectPath))
            case _ =>
                sparkSession.read.text(conf.objectPath)
        }

        dataframe*/

        return null;
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return RedisDataTunnelSourceOption.class;
    }
}
