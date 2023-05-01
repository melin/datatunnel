package com.superior.datatunnel.plugin.sftp;

import com.superior.datatunnel.common.annotation.OptionDesc;
import com.superior.datatunnel.common.enums.SaveMode;
import lombok.Data;

import javax.validation.constraints.NotNull;

@Data
public class SftpDataTunnelSinkOption extends SftpCommonOption {

    @OptionDesc("数据写入模式")
    @NotNull(message = "saveMode can not null")
    private SaveMode saveMode = SaveMode.APPEND;
}
