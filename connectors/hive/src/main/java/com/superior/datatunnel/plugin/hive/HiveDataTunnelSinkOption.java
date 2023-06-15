package com.superior.datatunnel.plugin.hive;

import com.superior.datatunnel.api.model.BaseSinkOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
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

    @OptionDesc("hive 表分区字段, 数据类型：String，只支持单分区表")
    private String partitionColumn;

    private String writeMode = "append";

    @OptionDesc("hive 表文件格式")
    private String format = "parquet";

    public String getFullTableName() {
        return databaseName + "." + tableName;
    }
}
