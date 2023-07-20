package com.superior.datatunnel.plugin.hive;

import com.superior.datatunnel.api.model.BaseSinkOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import com.superior.datatunnel.common.enums.Compression;
import com.superior.datatunnel.common.enums.FileFormat;
import com.superior.datatunnel.common.enums.WriteMode;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@Data
public class HiveDataTunnelSinkOption extends BaseSinkOption {

    @NotBlank(message = "databaseName can not blank")
    private String databaseName;

    @NotBlank(message = "tableName can not blank")
    private String tableName;

    @NotEmpty(message = "columns can not empty")
    private String[] columns;

    @OptionDesc("hive 表分区字段, 数据类型：String，只支持单分区表")
    private String partitionColumn;

    @OptionDesc("写入模式, 仅支持：append、overwrite，不支持 upsert")
    private WriteMode writeMode = WriteMode.OVERWRITE;

    @OptionDesc("hive 表文件格式，仅支持：orc、parquet、hudi、iceberg")
    @NotBlank(message = "format can not blank")
    private FileFormat fileFormat = FileFormat.PARQUET;

    @OptionDesc("写入文件压缩算法, 仅支持：SNAPPY, ZLIB, LZO, ZSTD, LZ4")
    @NotNull(message = "compression can not null")
    private Compression compression = Compression.ZSTD;

    @OptionDesc("hudi 表属性，自动建表时必须指定")
    private String primaryKey;

    @OptionDesc("hudi 表属性，自动建表时必须指定")
    private String preCombineField;

    public String getFullTableName() {
        return databaseName + "." + tableName;
    }
}
