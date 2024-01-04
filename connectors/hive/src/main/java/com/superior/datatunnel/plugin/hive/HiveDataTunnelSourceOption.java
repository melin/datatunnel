package com.superior.datatunnel.plugin.hive;

import com.superior.datatunnel.api.model.BaseSourceOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import lombok.Data;

import javax.validation.constraints.NotBlank;

@Data
public class HiveDataTunnelSourceOption extends BaseSourceOption {

    private String databaseName;

    @OptionDesc("等同 databaseName, databaseName 和 schemaName 只需设置一个")
    private String schemaName;

    @NotBlank(message = "tableName can not blank")
    private String tableName;

    private String condition;

    @OptionDesc("hive 表分区信息，例如：pt='20231201' and type='Login'，或者读取多个分区: pt>'20230718' and pt<='20230719'")
    private String partitionSpec;
}
