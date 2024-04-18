package com.superior.datatunnel.plugin.hdfs;

import com.superior.datatunnel.common.annotation.OptionDesc;
import com.superior.datatunnel.common.enums.Compression;
import com.superior.datatunnel.common.enums.WriteMode;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Data
public class HdfsDataTunnelSinkOption extends HdfsCommonOption {

    @OptionDesc("数据写入模式")
    @NotNull(message = "writeMode can not null")
    private WriteMode writeMode = WriteMode.APPEND;

    @OptionDesc("写入文件压缩算法, 仅支持：SNAPPY, ZLIB, LZO, ZSTD, LZ4")
    @NotNull(message = "compression can not null")
    private Compression compression = Compression.ZSTD;

    @NotBlank(message = "path can not blank")
    private String path;
}
