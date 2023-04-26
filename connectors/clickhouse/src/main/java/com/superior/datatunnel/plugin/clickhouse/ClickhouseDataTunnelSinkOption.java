package com.superior.datatunnel.plugin.clickhouse;

import com.gitee.melin.bee.core.hibernate5.validation.ValidConst;
import com.superior.datatunnel.api.model.BaseSinkOption;
import com.superior.datatunnel.common.annotation.SparkConfKey;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@Data
public class ClickhouseDataTunnelSinkOption extends BaseSinkOption {

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

    @SparkConfKey("spark.clickhouse.ignoreUnsupportedTransform")
    private boolean ignoreUnsupportedTransform;

    @SparkConfKey("spark.clickhouse.write.batchSize")
    private int batchSize = 10000;

    @SparkConfKey("spark.clickhouse.write.compression.codec")
    private String compressionCodec = "lz4";

    @SparkConfKey("spark.clickhouse.write.distributed.convertLocal")
    private boolean distributedConvertLocal = false;

    @SparkConfKey("spark.clickhouse.write.distributed.useClusterNodes")
    private boolean distributedUseClusterNodes = true;

    @SparkConfKey("spark.clickhouse.write.format")
    private String format = "arrow";

    @SparkConfKey("spark.clickhouse.write.localSortByKey")
    private boolean localSortByKey = true;

    @SparkConfKey("spark.clickhouse.write.localSortByPartition")
    private Boolean localSortByPartition;

    @SparkConfKey("spark.clickhouse.write.maxRetry")
    private int maxRetry = 3;

    @SparkConfKey("spark.clickhouse.write.repartitionByPartition")
    private boolean repartitionByPartition = true;

    @SparkConfKey("spark.clickhouse.write.repartitionNum")
    private int repartitionNum = 0;

    @SparkConfKey("spark.clickhouse.write.repartitionStrictly")
    private boolean repartitionStrictly = false;

    @SparkConfKey("spark.clickhouse.write.retryInterval")
    private String retryInterval = "10s";

    @SparkConfKey("spark.clickhouse.write.batchSize")
    private String retryableErrorCodes = "retryableErrorCodes";
}
