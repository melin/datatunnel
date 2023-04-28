package com.superior.datatunnel.plugin.s3;

import com.superior.datatunnel.common.annotation.OptionDesc;
import com.superior.datatunnel.common.enums.SaveMode;
import lombok.Data;

import javax.validation.constraints.NotNull;

@Data
public class S3DataTunnelSinkOption extends S3CommonOption {

    @OptionDesc("数据写入模式")
    @NotNull(message = "saveMode can not null")
    private SaveMode saveMode = SaveMode.APPEND;
}
