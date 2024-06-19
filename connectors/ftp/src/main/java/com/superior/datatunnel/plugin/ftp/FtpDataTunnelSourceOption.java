package com.superior.datatunnel.plugin.ftp;

import javax.validation.constraints.NotNull;
import lombok.Data;

@Data
public class FtpDataTunnelSourceOption extends FtpCommonOption {

    @NotNull(message = "paths can not null, 支持多个参数")
    private String[] paths;
}
