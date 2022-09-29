package com.superior.datatunnel.plugin.clickhouse;

import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@Data
public class ClickhouseDataTunnelSinkOption extends DataTunnelSinkOption {

    @NotBlank(message = "databaseName can not blank")
    private String databaseName;

    @NotBlank(message = "tableName can not blank")
    private String tableName;

    @NotEmpty(message = "columns can not empty")
    private String[] columns = new String[]{"*"};

    @NotBlank(message = "username can not blank")
    private String username;

    private String password;

    @NotBlank(message = "host can not blank")
    private String host;

    @NotNull(message = "port can not blank")
    private Integer port;

    private String protocol = "http";

    private int batchSize = 10000;

    private String compressionCodec = "lz4";

    private boolean distributedConvertLocal = false;

    private boolean distributedUseClusterNodes = true;

    private String format = "arrow";

    private boolean localSortByKey = true;

    private Boolean localSortByPartition;

    private int maxRetry = 3;

    private boolean repartitionByPartition = true;

    private int repartitionNum = 0;

    private boolean repartitionStrictly = false;

    private String retryInterval = "10s";

    private String retryableErrorCodes = "241";
}
