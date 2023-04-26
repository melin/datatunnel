package com.superior.datatunnel.plugin.s3;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import lombok.val;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class S3DataTunnelSource implements DataTunnelSource {

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        S3DataTunnelSourceOption sourceOption = (S3DataTunnelSourceOption) context.getSourceOption();

        System.setProperty(S3Configs.awsServicesEnableV4, "true");

        /*sparkSession.conf.set(S3Configs.accessId, conf.accessId);
        sparkSession.conf.set(S3Configs.secretKey, conf.secretKey);
        sparkSession.conf.set(S3Configs.s3aClientImpl, conf.s3aImpl);
        sparkSession.conf.set(S3Configs.sslEnabled, conf.sslEnabled);
        sparkSession.conf.set(S3Configs.endPoint, conf.endPoint);
        sparkSession.conf.set(S3Configs.pathStyleAccess, conf.pathStyleAccess);

        val dataframe = conf.objectFormat match {
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
        return S3DataTunnelSourceOption.class;
    }
}
