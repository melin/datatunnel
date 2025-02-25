package com.superior.datatunnel.plugin.kafka;

import com.superior.datatunnel.api.model.BaseSinkOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import com.superior.datatunnel.common.enums.OutputMode;
import javax.validation.constraints.NotBlank;

public class DatalakeDatatunnelSinkOption extends BaseSinkOption {

    private String databaseName;

    @OptionDesc("等同 databaseName, databaseName 和 schemaName 只需设置一个")
    private String schemaName;

    @NotBlank(message = "tableName can not blank")
    private String tableName;

    @OptionDesc("写入模式, 仅支持：append 和 complete")
    private OutputMode outputMode = OutputMode.APPEND;

    @OptionDesc("定义 delta/iceberg merge key，用于 merge sql")
    private String mergeColumns;

    @OptionDesc("iceberg 写入时，是否开启压缩")
    private boolean compactionEnabled = true;

    @OptionDesc("分区字段名, 多个逗号分割")
    private String partitionColumnNames;

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public @NotBlank(message = "tableName can not blank") String getTableName() {
        return tableName;
    }

    public void setTableName(@NotBlank(message = "tableName can not blank") String tableName) {
        this.tableName = tableName;
    }

    public OutputMode getOutputMode() {
        return outputMode;
    }

    public void setOutputMode(OutputMode outputMode) {
        this.outputMode = outputMode;
    }

    public String getMergeColumns() {
        return mergeColumns;
    }

    public void setMergeColumns(String mergeColumns) {
        this.mergeColumns = mergeColumns;
    }

    public String getPartitionColumnNames() {
        return partitionColumnNames;
    }

    public void setPartitionColumnNames(String partitionColumnNames) {
        this.partitionColumnNames = partitionColumnNames;
    }

    public boolean isCompactionEnabled() {
        return compactionEnabled;
    }

    public void setCompactionEnabled(boolean compactionEnabled) {
        this.compactionEnabled = compactionEnabled;
    }
}
