package com.superior.datatunnel.plugin.hdfs;

import com.superior.datatunnel.common.annotation.OptionDesc;
import com.superior.datatunnel.common.enums.Compression;
import com.superior.datatunnel.common.enums.WriteMode;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

import static com.superior.datatunnel.common.enums.FileFormat.*;

@Data
public class HdfsDataTunnelSinkOption extends HdfsCommonOption {

    @OptionDesc("数据写入模式")
    @NotNull(message = "writeMode can not null")
    private WriteMode writeMode = WriteMode.APPEND;

    @OptionDesc("文件压缩，不同文件格式, 支持压缩算法有差异，请参考spark 官方文档")
    @NotNull(message = "compression can not null")
    private Compression compression;

    @NotBlank(message = "path can not blank")
    private String path;

    public Compression getCompression() {
        if (compression == null) {
            if (this.getFormat() == PARQUET || this.getFormat() == ORC || this.getFormat() == HUDI ||
                    this.getFormat() == ICEBERG || this.getFormat() == PAIMON) {
                compression = Compression.ZSTD;
            } else if (this.getFormat() == AVRO) {
                compression = Compression.SNAPPY;
            } else {
                compression = Compression.NONE;
            }
        }

        return compression;
    }
}
