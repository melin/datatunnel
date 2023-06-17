package com.superior.datatunnel.plugin.hive;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.common.enums.WriteMode;
import com.superior.datatunnel.common.util.CommonUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

import static com.superior.datatunnel.common.enums.WriteMode.APPEND;
import static com.superior.datatunnel.common.enums.WriteMode.OVERWRITE;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class HiveDataTunnelSink implements DataTunnelSink {

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        HiveDataTunnelSinkOption sinkOption = (HiveDataTunnelSinkOption) context.getSinkOption();

        try {
            String sql = CommonUtils.genOutputSql(dataset, sinkOption.getColumns(), sinkOption.getTableName());
            dataset = context.getSparkSession().sql(sql);

            String tdlName = "tdl_datatunnel_" + System.currentTimeMillis();
            dataset.createTempView(tdlName);

            String databaseName = sinkOption.getDatabaseName();
            String tableName = sinkOption.getTableName();
            String partitionColumn = sinkOption.getPartitionColumn();
            WriteMode writeMode = sinkOption.getWriteMode();

            boolean isPartition = HiveUtils.checkPartition(context.getSparkSession(), databaseName, tableName);
            if (isPartition && StringUtils.isBlank(partitionColumn)) {
                throw new DataTunnelException("写入表为分区表，请指定写入分区");
            }

            String table = tableName;
            if (StringUtils.isNotBlank(databaseName)) {
                table = databaseName + "." + tableName;
            }

            if (APPEND == writeMode) {
                if (isPartition) {
                    sql = "insert into table " + table + " partition(" + partitionColumn + ") select * from " + tdlName;
                } else {
                    sql = "insert into table " + table + " select * from " + tdlName;
                }
            } else if (OVERWRITE == writeMode) {
                if (isPartition) {
                    sql = "insert overwrite table " + table + " partition(" + partitionColumn + ") select * from " + tdlName;
                } else {
                    sql = "insert overwrite table " + table + " select * from " + tdlName;
                }
            } else {
                throw new DataTunnelException("不支持的写入模式：" + writeMode);
            }

            context.getSparkSession().sql(sql);
        } catch (Exception e) {
            throw new DataTunnelException(e.getMessage(), e);
        }
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return HiveDataTunnelSinkOption.class;
    }
}
