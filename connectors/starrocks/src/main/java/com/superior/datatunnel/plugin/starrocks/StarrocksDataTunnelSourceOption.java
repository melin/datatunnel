package com.superior.datatunnel.plugin.starrocks;

import com.superior.datatunnel.api.model.BaseSourceOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import lombok.Data;

import javax.validation.constraints.NotBlank;

@Data
public class StarrocksDataTunnelSourceOption extends BaseSourceOption {

    @OptionDesc("数据库名")
    @NotBlank
    private String databaseName;

    @OptionDesc("表名")
    @NotBlank
    private String tableName;

    @OptionDesc("StarRocks 集群账号的用户名")
    @NotBlank
    private String user;

    @OptionDesc("StarRocks 集群账号的密码")
    @NotBlank
    private String password;

    private String host;

    private Integer port;

    @OptionDesc("jdbc 连接地址，如果填写jdbcUrl, 就不需要填写host & port")
    private String jdbcUrl;

    @OptionDesc("FE 的 HTTP 地址，支持输入多个FE地址，使用逗号分隔")
    @NotBlank
    private String feEnpoints;
}
