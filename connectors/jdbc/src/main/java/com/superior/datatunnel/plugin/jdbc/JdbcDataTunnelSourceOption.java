package com.superior.datatunnel.plugin.jdbc;

import com.superior.datatunnel.api.model.BaseSourceOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import lombok.Data;

import javax.validation.constraints.NotBlank;

@Data
public class JdbcDataTunnelSourceOption extends BaseSourceOption {

    @OptionDesc("数据库名")
    private String databaseName;

    @OptionDesc("数据库 schema 名，如果是mysql或者oracle，databaseName和schemaName 任意填写一个。" +
            "支持多个schema名称，逗号分隔。支持正则表达式")
    private String schemaName;

    @OptionDesc("支持多个tableName，逗号分隔。支持正则表达式")
    @NotBlank(message = "tableName can not blank")
    private String tableName;

    @NotBlank(message = "username can not blank")
    private String username;

    private String password;

    private String host;

    private Integer port;

    @OptionDesc("jdbc 连接地址，如果填写jdbcUrl, 就不需要填写host & port")
    private String jdbcUrl;

    private int fetchsize = 1000;

    private int queryTimeout = 0;

    @OptionDesc("数据过滤条件")
    private String condition;

    @OptionDesc("切片字段")
    private String partitionColumn;

    @OptionDesc("每切片记录数量，用于计算切片数量，如果指定numPartitions，该值不生效, 默认：100000")
    private Integer partitionRecordCount = 100000;

    private Integer numPartitions;

    private String lowerBound;

    private String upperBound;

    private boolean pushDownPredicate = true;

    private boolean pushDownAggregate = true;

    private boolean pushDownLimit = true;
}
