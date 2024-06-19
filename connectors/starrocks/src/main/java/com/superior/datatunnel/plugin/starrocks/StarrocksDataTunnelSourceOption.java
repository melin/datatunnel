package com.superior.datatunnel.plugin.starrocks;

import com.superior.datatunnel.api.model.BaseSourceOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import javax.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class StarrocksDataTunnelSourceOption extends BaseSourceOption {

    private String databaseName;

    @OptionDesc("等同 databaseName, databaseName 和 schemaName 只需设置一个")
    private String schemaName;

    @OptionDesc("表名")
    @NotBlank
    private String tableName;

    @OptionDesc("StarRocks 集群账号的用户名")
    @NotBlank
    private String username;

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
