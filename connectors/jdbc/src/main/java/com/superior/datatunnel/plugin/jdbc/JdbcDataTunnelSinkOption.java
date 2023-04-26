package com.superior.datatunnel.plugin.jdbc;

import com.superior.datatunnel.api.model.BaseSinkOption;
import com.superior.datatunnel.common.annotation.SparkConfDesc;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@Data
public class JdbcDataTunnelSinkOption extends BaseSinkOption {

    @SparkConfDesc("数据库名")
    private String databaseName;

    @SparkConfDesc("数据库 schema 名，如果是mysql或者oracle，databaseName和schemaName 任意填写一个")
    private String schemaName;

    @SparkConfDesc("数据库表名")
    @NotBlank(message = "tableName can not blank")
    private String tableName;

    @SparkConfDesc("oracle sid。sid和serviceName，只能选择填写一个")
    private String sid;

    @SparkConfDesc("oracle serviceName。sid和serviceName，只能选择填写一个")
    private String serviceName;

    @NotEmpty(message = "columns can not empty")

    private String[] columns = new String[]{"*"};

    @NotBlank(message = "username can not blank")
    private String username;

    private String password;

    @NotBlank(message = "host can not blank")
    private String host;

    @NotNull(message = "port can not blank")
    private Integer port;

    @NotBlank(message = "数据写入模式，支持：overwrite、insert、upsert")
    @NotBlank
    private String writeMode = "insert";

    private int batchsize = 1000;

    private int queryTimeout = 0;

    private boolean truncate = false;

    private String preSql;

    private String postSql;

    private String isolationLevel = "READ_UNCOMMITTED";

    public String getFullTableName() {
        return databaseName + "." + tableName;
    }
}
