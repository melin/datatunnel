package com.superior.datatunnel.plugin.mongodb;

import com.superior.datatunnel.api.model.BaseSinkOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import com.superior.datatunnel.common.enums.WriteMode;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import lombok.Data;

import static com.mongodb.spark.sql.connector.config.MongoConfig.CLIENT_FACTORY_DEFAULT;

// https://www.mongodb.com/docs/spark-connector/current/batch-mode/batch-write-config/
@Data
public class MongodbDataTunnelSinkOption extends BaseSinkOption {

    @OptionDesc("数据写入模式")
    @NotNull(message = "writeMode can not null, only support: append, overwrite")
    private WriteMode writeMode = WriteMode.APPEND;

    @NotBlank(message = "connection uri can not blank")
    @OptionDesc("The connection string configuration key. Default: mongodb://localhost:27017/")
    private String connectionUri = "mongodb://localhost:27017/";

    @NotBlank(message = "database can not blank")
    @OptionDesc("The database name configuration.")
    private String database;

    @NotBlank(message = "collection can not blank")
    @OptionDesc("The collection name configuration.")
    private String collection;

    @OptionDesc("The comment to append to the write operation. Comments appear in the output of the Database Profiler.")
    private String comment;

    @OptionDesc(
            "MongoClientFactory configuration key. Default: com.mongodb.spark.sql.connector.connection.DefaultMongoClientFactory")
    private String mongoClientFactory = CLIENT_FACTORY_DEFAULT;

    @OptionDesc("Specifies whether the connector parses the string and converts extended JSON into BSON.")
    private String convertJson = "false";

    @OptionDesc(
            "Field or list of fields by which to split the collection data. To specify more than one field, separate them using a comma as shown in the following example: fieldName1,fieldName2 ")
    private String idFieldList = "_id";

    @OptionDesc(
            "When true, the connector ignores any null values when writing, including null values in arrays and nested documents.")
    private boolean ignoreNullValues = false;

    @OptionDesc("Specifies the maximum number of operations to batch in bulk operations.")
    private Integer maxBatchSize = 512;

    @NotBlank(message = "operationType can not blank")
    @OptionDesc(
            "Specifies the type of write operation to perform. You can set this to one of the following values: insert, replace, update")
    private String operationType = "replace";

    @OptionDesc("Specifies whether to perform ordered bulk operations.")
    private boolean ordered = true;

    @OptionDesc("When true, replace and update operations will insert the data if no match exists.\n"
            + "For time series collections, you must set upsertDocument to false..")
    private boolean upsertDocument = true;
}
