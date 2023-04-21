package com.superior.datatunnel.plugin.cassandra;

import com.gitee.melin.bee.core.hibernate5.validation.ValidConst;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import com.superior.datatunnel.common.annotation.SparkConfKey;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@Data
public class CassandraDataTunnelSourceOption extends DataTunnelSourceOption {

    @NotBlank(message = "databaseName can not blank")
    private String databaseName;

    @NotBlank(message = "tableName can not blank")
    private String tableName;

    @NotEmpty(message = "columns can not empty")
    private String[] columns = new String[]{"*"};

    @NotBlank(message = "username can not blank")
    @SparkConfKey("spark.sql.catalog.clickhouse.user")
    private String username;

    @SparkConfKey("spark.sql.catalog.clickhouse.password")
    private String password = "";

    @NotBlank(message = "host can not blank")
    @SparkConfKey("spark.sql.catalog.clickhouse.host")
    private String host;

    @NotNull(message = "port can not blank")
    private Integer port;

    @ValidConst({"http", "grpc"})
    private String protocol = "http";

    private String condition;

    @SparkConfKey("spark.clickhouse.ignoreUnsupportedTransform")
    private boolean ignoreUnsupportedTransform;

    @SparkConfKey("spark.clickhouse.read.compression.codec")
    private String compressionCodec = "lz4";

    @SparkConfKey("spark.clickhouse.read.distributed.convertLocal")
    private boolean distributedConvertLocal = true;

    @SparkConfKey("spark.clickhouse.read.format")
    private String format = "json";

    @SparkConfKey("spark.clickhouse.read.splitByPartitionId")
    private boolean splitByPartitionId = true;
}
