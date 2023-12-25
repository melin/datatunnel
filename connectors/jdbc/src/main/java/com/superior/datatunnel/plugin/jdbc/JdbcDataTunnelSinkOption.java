package com.superior.datatunnel.plugin.jdbc;

import com.superior.datatunnel.api.model.BaseSinkOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import com.superior.datatunnel.common.enums.WriteMode;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Data
public class JdbcDataTunnelSinkOption extends BaseSinkOption {

    @OptionDesc("数据库名")
    private String databaseName;

    @OptionDesc("数据库 schema 名，如果是mysql或者oracle，databaseName和schemaName 任意填写一个")
    private String schemaName;

    @OptionDesc("数据库表名")
    @NotBlank(message = "tableName can not blank")
    private String tableName;

    @OptionDesc("oracle sid。sid和serviceName，只能选择填写一个")
    private String sid;

    @OptionDesc("oracle serviceName。sid和serviceName，只能选择填写一个")
    private String serviceName;

    @NotBlank(message = "username can not blank")
    private String username;

    private String password;

    private String host;

    private Integer port;

    @OptionDesc("jdbc 连接地址，如果填写jdbcUrl, 就不需要填写host & port")
    private String jdbcUrl;

    @OptionDesc("数据写入模式")
    @NotNull(message = "writeMode can not null")
    private WriteMode writeMode = WriteMode.APPEND;

    private int batchsize = 1024;

    private int queryTimeout = 0;

    private boolean truncate = true;

    private String preActions;

    private String postActions;

    private String isolationLevel = "READ_UNCOMMITTED";

    public String getFullTableName() {
        return databaseName + "." + tableName;
    }
}
