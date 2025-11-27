package com.superior.datatunnel.plugin.oceanbase;

import com.superior.datatunnel.api.model.BaseSinkOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import javax.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class OceanBaseDataTunnelSinkOption extends BaseSinkOption {

    @NotBlank(message = "schemaName can not blank")
    private String schemaName;

    @NotBlank(message = "tableName can not blank")
    private String tableName;

    @NotBlank(message = "username can not blank")
    private String username;

    @NotBlank(message = "password can not blank")
    private String password;

    @OptionDesc("jdbc 连接地址，如果填写jdbcUrl, 就不需要填写host & port")
    private String jdbcUrl;
}
