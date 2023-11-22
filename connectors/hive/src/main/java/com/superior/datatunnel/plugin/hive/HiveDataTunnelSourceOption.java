package com.superior.datatunnel.plugin.hive;

import com.superior.datatunnel.api.model.BaseSourceOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import lombok.Data;

import javax.validation.constraints.NotBlank;

@Data
public class HiveDataTunnelSourceOption extends BaseSourceOption {

    @NotBlank(message = "databaseName can not blank")
    private String databaseName;

    @NotBlank(message = "tableName can not blank")
    private String tableName;

    private String condition;

    @OptionDesc("hive 表分区信息，例如：pt='20231201', type='Login'")
    private String partitionSpec;
}
