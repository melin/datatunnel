package com.superior.datatunnel.plugin.redshift;

import com.superior.datatunnel.api.model.BaseSinkOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import com.superior.datatunnel.common.enums.WriteMode;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import lombok.Data;

@Data
public class SnowflakeDataTunnelSinkOption extends BaseSinkOption {

    private String warehouse;

    @OptionDesc("数据库名")
    @NotBlank(message = "databaseName can not blank")
    private String databaseName;

    @OptionDesc("数据库 schema 名")
    @NotBlank(message = "schemaName can not blank")
    private String schemaName;

    @OptionDesc("数据库表名")
    @NotBlank(message = "tableName can not blank")
    private String tableName;

    private String username;

    private String password = "";

    @NotBlank(message = "host can not blank")
    private String host;

    @OptionDesc("数据写入模式")
    @NotNull(message = "writeMode can not null")
    private WriteMode writeMode = WriteMode.APPEND;
}
