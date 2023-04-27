package com.superior.datatunnel.plugin.jdbc;

import com.superior.datatunnel.api.model.BaseSourceOption;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@Data
public class JdbcDataTunnelSourceOption extends BaseSourceOption {

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

    private String password;

    @NotBlank(message = "host can not blank")
    private String host;

    @NotNull(message = "port can not blank")
    private Integer port;

    private int fetchSize = 1000;

    private int queryTimeout = 0;

    private String condition;

    private String partitionColumn;

    private Integer numPartitions;

    private String lowerBound;

    private String upperBound;

    private boolean pushDownPredicate = true;

    private boolean pushDownAggregate = true;

    private boolean pushDownLimit = true;
}