package com.superior.datatunnel.plugin.hive;

import com.superior.datatunnel.api.model.BaseSourceOption;
import lombok.Data;

import javax.validation.constraints.NotBlank;

@Data
public class HiveDataTunnelSourceOption extends BaseSourceOption {

    @NotBlank(message = "databaseName can not blank")
    private String databaseName;

    @NotBlank(message = "tableName can not blank")
    private String tableName;

    private String condition;
}
