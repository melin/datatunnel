package com.superior.datatunnel.plugin.cassandra;

import com.superior.datatunnel.api.model.BaseSinkOption;
import com.superior.datatunnel.common.annotation.SparkConfKey;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import lombok.Data;

@Data
public class CassandraDataTunnelSinkOption extends BaseSinkOption {

    @NotBlank(message = "keyspace can not blank")
    private String keyspace;

    @NotBlank(message = "tableName can not blank")
    private String tableName;

    @NotBlank(message = "username can not blank")
    @SparkConfKey("spark.cassandra.auth.username")
    private String username;

    @SparkConfKey("spark.cassandra.auth.password")
    private String password = "";

    @NotBlank(message = "host can not blank")
    @SparkConfKey("spark.cassandra.connection.host")
    private String host;

    @NotNull(message = "port can not blank")
    @SparkConfKey("spark.cassandra.connection.port")
    private Integer port = 9402;

    // sink
    @SparkConfKey("spark.cassandra.output.batch.grouping.buffer.size")
    private Integer batchGroupingBufferSize = 1000;

    @SparkConfKey("spark.cassandra.output.batch.grouping.key")
    private String batchGroupingKey = "partition";

    @SparkConfKey("spark.cassandra.output.batch.size.bytes")
    private Integer batchSizeBytes = 1024;

    @SparkConfKey("spark.cassandra.output.batch.size.rows")
    private Integer batchSizeRows;

    @SparkConfKey("spark.cassandra.output.concurrent.writes")
    private Integer concurrentWrites = 5;

    @SparkConfKey("spark.cassandra.output.consistency.level")
    private String concurrentLevel = "LOCAL_QUORUM";

    @SparkConfKey("spark.cassandra.output.ifNotExists")
    private Boolean ifNotExists = false;

    @SparkConfKey("spark.cassandra.output.ignoreNulls")
    private Boolean ignoreNulls = false;

    @SparkConfKey("spark.cassandra.output.metrics")
    private Boolean metrics = true;

    @SparkConfKey("spark.cassandra.output.throughputMBPerSec")
    private Integer throughputMBPerSec;

    @SparkConfKey("spark.cassandra.output.timestamp")
    private Integer timestamp = 0;

    @SparkConfKey("spark.cassandra.output.timestamp")
    private Integer ttl = 0;
}
