package com.superior.datatunnel.plugin.redshift;

import com.superior.datatunnel.api.model.BaseSourceOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import lombok.Data;

@Data
public class SnowflakeDataTunnelSourceOption extends BaseSourceOption {

    private String warehouse;

    @OptionDesc("数据库名")
    @NotBlank(message = "databaseName can not blank")
    private String databaseName;

    @OptionDesc("数据库 schema 名")
    private String schemaName;

    @OptionDesc("数据库表名")
    private String tableName;

    private String query;

    @NotEmpty(message = "columns can not empty")
    private String[] columns = new String[] {"*"};

    private String username;

    private String password = "";

    @NotBlank(message = "host can not blank")
    private String host;
}
