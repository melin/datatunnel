package com.superior.datatunnel.plugin.mongodb;

import static com.mongodb.spark.sql.connector.config.MongoConfig.CLIENT_FACTORY_DEFAULT;
import static com.mongodb.spark.sql.connector.config.ReadConfig.PARTITIONER_DEFAULT;

import com.superior.datatunnel.api.model.BaseSourceOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import javax.validation.constraints.NotBlank;
import lombok.Data;

// https://www.mongodb.com/docs/spark-connector/current/batch-mode/batch-read-config/
@Data
public class MongodbDataTunnelSourceOption extends BaseSourceOption {

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

    @OptionDesc(
            "The partitioner full class name. Default: com.mongodb.spark.sql.connector.read.partitioner.SamplePartitioner")
    private String partitioner = PARTITIONER_DEFAULT;

    @OptionDesc("The number of documents to sample from the collection when inferring the schema. Default: 1000")
    private Integer sampleSize = 1000;

    private String condition;
}
