package com.superior.datatunnel.plugin.mongodb;

import static com.mongodb.spark.sql.connector.config.MongoConfig.*;
import static com.mongodb.spark.sql.connector.config.MongoConfig.CLIENT_FACTORY_CONFIG;
import static com.mongodb.spark.sql.connector.config.WriteConfig.*;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class MongodbDataTunnelSink implements DataTunnelSink {

    @Override
    public void sink(Dataset<Row> dataFrame, DataTunnelContext context) throws IOException {
        MongodbDataTunnelSinkOption option = (MongodbDataTunnelSinkOption) context.getSinkOption();

        DataFrameWriter dfWriter = dataFrame
                .write()
                .format("mongodb")
                .mode(option.getWriteMode().name().toLowerCase())
                .option(CONNECTION_STRING_CONFIG, option.getConnectionUri())
                .option(DATABASE_NAME_CONFIG, option.getDatabase())
                .option(COLLECTION_NAME_CONFIG, option.getCollection())
                .option(CLIENT_FACTORY_CONFIG, option.getMongoClientFactory())
                .option(CONVERT_JSON_CONFIG, option.getConvertJson())
                .option(ID_FIELD_CONFIG, option.getIdFieldList())
                .option(IGNORE_NULL_VALUES_CONFIG, option.isIgnoreNullValues())
                .option(MAX_BATCH_SIZE_CONFIG, option.getMaxBatchSize())
                .option(OPERATION_TYPE_CONFIG, option.getOperationType())
                .option(ORDERED_BULK_OPERATION_CONFIG, option.isOrdered())
                .option(UPSERT_DOCUMENT_CONFIG, option.isUpsertDocument());

        if (StringUtils.isNotBlank(option.getComment())) {
            dfWriter.option(COMMENT_CONFIG, option.getComment());
        }

        option.getProperties().forEach(dfWriter::option);

        dfWriter.save();
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return MongodbDataTunnelSinkOption.class;
    }
}
