package com.superior.datatunnel.hive;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.common.util.CommonUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class HiveSink implements DataTunnelSink {

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        HiveSinkOption sinkOption = (HiveSinkOption) context.getSinkOption();

        try {
            String sql = CommonUtils.genOutputSql(dataset, sinkOption.getColumns(), sinkOption.getTableName());
            dataset = context.getSparkSession().sql(sql);

            String tdlName = "tdl_datax_" + System.currentTimeMillis();
            dataset.createTempView(tdlName);

            String databaseName = sinkOption.getDatabaseName();
            String tableName = sinkOption.getTableName();
            String partition = sinkOption.getPartition();
            String writeMode = sinkOption.getWriteMode();

            boolean isPartition = HiveUtils.checkPartition(context.getSparkSession(), databaseName, tableName);
            if (isPartition && StringUtils.isBlank(partition)) {
                throw new DataTunnelException("写入表为分区表，请指定写入分区");
            }

            String table = tableName;
            if (StringUtils.isNotBlank(databaseName)) {
                table = databaseName + "." + tableName;
            }

            if ("append".equals(writeMode)) {
                if (isPartition) {
                    sql = "insert into table " + table + " partition(" + partition + ") select * from " + tdlName;
                } else {
                    sql = "insert into table " + table + " select * from " + tdlName;
                }
            } else {
                if (isPartition) {
                    sql = "insert overwrite table " + table + " partition(" + partition + ") select * from " + tdlName;
                } else {
                    sql = "insert overwrite table " + table + " select * from " + tdlName;
                }
            }

            context.getSparkSession().sql(sql);
        } catch (Exception e) {
            throw new DataTunnelException(e.getMessage(), e);
        }
    }
}
