package com.superior.datatunnel.plugin.ftp;

import com.superior.datatunnel.api.model.BaseCommonOption;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import com.superior.datatunnel.common.enums.FileFormat;
import com.superior.datatunnel.plugin.ftp.enums.AuthType;
import com.superior.datatunnel.plugin.ftp.enums.FtpProtocol;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@Data
public class FtpCommonOption extends BaseCommonOption
        implements DataTunnelSourceOption, DataTunnelSinkOption {

    //ftp 协议：ftp、sftp
    @NotNull(message = "protocol can not null")
    @OptionDesc("ftp 协议：ftp、sftp")
    private FtpProtocol protocol = FtpProtocol.FTP;

    //sftp 认证方式
    @NotNull(message = "protocol can not null")
    @OptionDesc("sftp 认证方式: password, sshkey")
    private AuthType authType = AuthType.PASSWORD;

    @NotBlank(message = "username can not blank")
    private String username;

    private String password;

    @NotBlank(message = "host can not blank")
    private String host;

    @NotNull(message = "port can not blank")
    private Integer port = 21;

    @NotNull(message = "format can not null")
    private FileFormat format;

    @OptionDesc("sftp keys file")
    private String sshKeyFile;

    private String sshPassphrase = "passphrase";

    @OptionDesc("csv 字段分隔符")
    private String sep = ",";

    @OptionDesc("csv 文件编码")
    private String encoding = "UTF-8";

    @OptionDesc("csv 文件，第一行是否为字段名")
    private boolean header = false;

    @NotEmpty(message = "columns can not empty")
    private String[] columns = new String[]{"*"};

    @Override
    public String[] getColumns() {
        return columns;
    }
}
