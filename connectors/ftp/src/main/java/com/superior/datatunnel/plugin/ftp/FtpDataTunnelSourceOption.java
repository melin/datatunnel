package com.superior.datatunnel.plugin.ftp;

import com.superior.datatunnel.common.annotation.OptionDesc;
import lombok.Data;

@Data
public class FtpDataTunnelSourceOption extends FtpCommonOption {

    @OptionDesc("csv 字段分隔符")
    private String sep;
}
