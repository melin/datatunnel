package com.superior.datatunnel.plugin.hive;

import com.superior.datatunnel.api.model.BaseSinkOption;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;

@Data
public class HiveDataTunnelSinkOption extends BaseSinkOption {

    @NotBlank(message = "databaseName can not blank")
    private String databaseName;

    @NotBlank(message = "tableName can not blank")
    private String tableName;

    @NotEmpty(message = "columns can not empty")
    private String[] columns;

    private String partition;

    private String writeMode = "append";

    public String getFullTableName() {
        return databaseName + "." + tableName;
    }
}
