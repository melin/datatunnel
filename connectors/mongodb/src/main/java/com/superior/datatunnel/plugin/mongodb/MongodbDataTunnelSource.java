package com.superior.datatunnel.plugin.mongodb;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

import static com.mongodb.spark.sql.connector.config.MongoConfig.*;
import static com.mongodb.spark.sql.connector.config.ReadConfig.INFER_SCHEMA_SAMPLE_SIZE_CONFIG;
import static com.mongodb.spark.sql.connector.config.ReadConfig.PARTITIONER_CONFIG;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class MongodbDataTunnelSource implements DataTunnelSource {

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        SparkSession sparkSession = context.getSparkSession();
        MongodbDataTunnelSourceOption option = (MongodbDataTunnelSourceOption) context.getSourceOption();

        DataFrameReader reader = sparkSession.read().format("mongodb")
                .option(CONNECTION_STRING_CONFIG, option.getConnectionUri())
                .option(DATABASE_NAME_CONFIG, option.getDatabase())
                .option(COLLECTION_NAME_CONFIG, option.getCollection())
                .option(CLIENT_FACTORY_CONFIG, option.getMongoClientFactory())
                .option(PARTITIONER_CONFIG, option.getPartitioner())
                .option(INFER_SCHEMA_SAMPLE_SIZE_CONFIG, option.getSampleSize());

        if (StringUtils.isNotBlank(option.getComment())) {
            reader.option(COMMENT_CONFIG, option.getComment());
        }

        option.getProperties().forEach(reader::option);

        Dataset<Row> df = reader.load();

        if (StringUtils.isNotBlank(option.getCondition())) {
            try {
                String viewName = "datatunnel_mongodb_" + System.currentTimeMillis();
                df.createTempView(viewName);
                String sql = "select * from " + viewName + " where " + option.getCondition();
                df = sparkSession.sql(sql);
            } catch (Exception e) {
                throw new DataTunnelException("create hbase view: " + e.getMessage(), e);
            }
        }

        return df;
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return MongodbDataTunnelSourceOption.class;
    }
}
