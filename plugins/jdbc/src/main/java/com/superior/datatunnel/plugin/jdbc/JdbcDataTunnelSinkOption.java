package com.superior.datatunnel.plugin.jdbc;

import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@Data
public class JdbcDataTunnelSinkOption extends DataTunnelSinkOption {

    private String databaseName;

    private String schemaName;

    @NotBlank(message = "tableName can not blank")
    private String tableName;

    //oracle
    private String sid;

    //oracle
    private String serviceName;

    @NotEmpty(message = "columns can not empty")
    private String[] columns = new String[]{"*"};

    @NotBlank(message = "username can not blank")
    private String username;

    @NotBlank(message = "password can not blank")
    private String password;

    @NotBlank(message = "host can not blank")
    private String host;

    @NotNull(message = "port can not blank")
    private Integer port;

    // overwrite/insert/upsert
    @NotBlank
    private String writeMode = "insert";

    private int batchsize = 1000;

    private int queryTimeout = 0;

    private boolean truncate = false;

    private String preSql;

    private String postSql;

    public String getFullTableName() {
        return databaseName + "." + tableName;
    }
}
