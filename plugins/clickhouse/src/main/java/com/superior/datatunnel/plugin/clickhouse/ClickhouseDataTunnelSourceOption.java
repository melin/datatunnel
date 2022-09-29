package com.superior.datatunnel.plugin.clickhouse;

import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@Data
public class ClickhouseDataTunnelSourceOption extends DataTunnelSourceOption {

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

    private boolean ignoreUnsupportedTransform;

    private String compressionCodec = "lz4";

    private boolean distributedConvertLocal = true;

    private String format = "json";

    private boolean splitByPartitionId = true;
}
