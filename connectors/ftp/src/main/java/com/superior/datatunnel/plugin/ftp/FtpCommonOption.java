package com.superior.datatunnel.plugin.ftp;

import com.superior.datatunnel.api.model.BaseCommonOption;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import com.superior.datatunnel.common.enums.FileFormat;
import com.superior.datatunnel.plugin.ftp.enums.DataConnectionMode;
import com.superior.datatunnel.plugin.ftp.enums.TransferMode;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Data
public class FtpCommonOption extends BaseCommonOption
        implements DataTunnelSourceOption, DataTunnelSinkOption {

    @NotBlank(message = "username can not blank")
    private String username;

    @NotBlank(message = "password can not blank")
    private String password;

    @NotBlank(message = "host can not blank")
    private String host;

    @NotNull(message = "port can not blank")
    private Integer port = 21;

    private Integer keepAliveTimeout = 0;

    @NotNull(message = "connectionMode can not blank")
    private DataConnectionMode connectionMode = DataConnectionMode.LOCAL;

    private TransferMode transferMode = TransferMode.BLOCK;

    @NotNull(message = "format can not null")
    private FileFormat format;

    @NotBlank(message = "filePath can not blank")
    private String filePath;

}
