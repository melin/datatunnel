package com.superior.datatunnel.plugin.cassandra;

import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import com.superior.datatunnel.common.annotation.SparkConfKey;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@Data
public class CassandraDataTunnelSourceOption extends DataTunnelSourceOption {

    @NotBlank(message = "keyspace can not blank")
    private String keyspace;

    @NotBlank(message = "tableName can not blank")
    private String tableName;

    @NotEmpty(message = "columns can not empty")
    private String[] columns = new String[]{"*"};

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

    private String condition;

    // read
    @SparkConfKey("spark.cassandra.concurrent.reads")
    private Integer concurrentReads = 512;

    @SparkConfKey("spark.cassandra.input.consistency.level")
    private String consistencyLevel = "LOCAL_ONE";

    @SparkConfKey("spark.cassandra.input.fetch.sizeInRows")
    private Integer fetchSizeInRows = 1000;

    @SparkConfKey("spark.cassandra.input.metrics")
    private Boolean metrics = true;

    @SparkConfKey("spark.cassandra.input.readsPerSec")
    private Integer readsPerSec;

    @SparkConfKey("spark.cassandra.input.split.sizeInMB")
    private Integer splitSizeInMB = 512;

    @SparkConfKey("spark.cassandra.input.throughputMBPerSec")
    private Integer throughputMBPerSec;
}
