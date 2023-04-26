package com.superior.datatunnel.plugin.hive;

import com.superior.datatunnel.api.model.BaseSourceOption;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;

@Data
public class HiveDataTunnelSourceOption extends BaseSourceOption {

    @NotBlank(message = "databaseName can not blank")
    private String databaseName;

    @NotBlank(message = "tableName can not blank")
    private String tableName;

    @NotEmpty(message = "columns can not empty")
    private String[] columns;

    private String condition;
}
