package com.superior.datatunnel.plugin.s3;

import com.superior.datatunnel.common.annotation.OptionDesc;
import com.superior.datatunnel.common.enums.WriteMode;
import lombok.Data;

import javax.validation.constraints.NotNull;

@Data
public class S3DataTunnelSinkOption extends S3CommonOption {

    @OptionDesc("文件压缩，支持：none, bzip2, gzip, lz4, snappy and deflate")
    private String compression = "none";

    @OptionDesc("数据写入模式")
    @NotNull(message = "writeMode can not null")
    private WriteMode writeMode = WriteMode.APPEND;

    @NotNull(message = "path can not blank")
    private String path;
}
