package com.superior.datatunnel.plugin.doris;

import com.superior.datatunnel.api.model.BaseSourceOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import javax.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class DorisDataTunnelSourceOption extends BaseSourceOption {

    @OptionDesc("数据库名")
    @NotBlank
    private String databaseName;

    @OptionDesc("表名")
    @NotBlank
    private String tableName;

    @OptionDesc("doris 集群账号的用户名")
    @NotBlank
    private String username;

    @OptionDesc("doris 集群账号的密码")
    private String password;

    @OptionDesc("FE 的 HTTP 地址，支持输入多个FE地址，使用逗号分隔")
    @NotBlank
    private String feEnpoints;
}
