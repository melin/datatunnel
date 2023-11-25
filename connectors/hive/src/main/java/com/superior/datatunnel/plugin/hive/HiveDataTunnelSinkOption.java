package com.superior.datatunnel.plugin.hive;

import com.superior.datatunnel.api.model.BaseSinkOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import com.superior.datatunnel.common.enums.Compression;
import com.superior.datatunnel.common.enums.FileFormat;
import com.superior.datatunnel.common.enums.WriteMode;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Data
public class HiveDataTunnelSinkOption extends BaseSinkOption {

    private String databaseName;

    @OptionDesc("等同 databaseName, databaseName 和 schemaName 只需设置一个")
    private String schemaName;

    @NotBlank(message = "tableName can not blank")
    private String tableName;

    @OptionDesc("hive 表分区信息，例如：pt='20231201', type='Login'")
    private String partitionSpec;

    @OptionDesc("写入模式, 仅支持：append、overwrite，不支持 upsert")
    private WriteMode writeMode = WriteMode.OVERWRITE;

    @OptionDesc("hive 表文件格式，仅支持：orc、parquet、hudi、iceberg")
    @NotNull(message = "format can not blank")
    private FileFormat fileFormat = FileFormat.PARQUET;

    @OptionDesc("写入文件压缩算法, 仅支持：SNAPPY, ZLIB, LZO, ZSTD, LZ4")
    @NotNull(message = "compression can not null")
    private Compression compression = Compression.ZSTD;

    @OptionDesc("hudi 表属性，自动建表时必须指定")
    private String primaryKey;

    @OptionDesc("hudi 表属性，自动建表时必须指定")
    private String preCombineField;

    @OptionDesc("主要控制输出文件数量。具体参考：https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-hints.html")
    private Integer rebalance;

    public String getFullTableName() {
        return databaseName + "." + tableName;
    }
}
