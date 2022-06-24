package com.superior.datatunnel.sftp;

import com.superior.datatunnel.api.model.SinkOption;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Data
public class SftpSinkOption extends SinkOption {

    @NotBlank(message = "path can not blank")
    private String path;

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

    private boolean overwrite;
}
