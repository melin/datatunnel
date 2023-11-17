package com.superior.datatunnel.plugin.sftp;

import com.superior.datatunnel.api.model.BaseCommonOption;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import com.superior.datatunnel.common.enums.FileFormat;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@Data
public class SftpCommonOption extends BaseCommonOption
        implements DataTunnelSourceOption, DataTunnelSinkOption {

    @NotBlank(message = "username can not blank")
    private String username;

    @NotBlank(message = "password can not blank")
    private String password;

    @NotBlank(message = "host can not blank")
    private String host;

    @NotNull(message = "port can not blank")
    private Integer port;

    private String keyFilePath;

    private String passPhrase;

    @NotNull(message = "format can not null")
    private FileFormat format;

    @NotBlank(message = "filePath can not blank")
    private String filePath;

    @NotEmpty(message = "columns can not empty")
    private String[] columns = new String[]{"*"};

    @Override
    public String[] getColumns() {
        return columns;
    }
}
