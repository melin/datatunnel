package com.superior.datatunnel.hive;

import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.common.util.CommonUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Map;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class HiveWriter implements DataTunnelSink {

    @Override
    public void validateOptions(Map<String, String> options) {
    }

    @Override
    public void write(SparkSession sparkSession, Dataset<Row> dataset, Map<String, String> options) throws IOException {
        try {
            String sql = CommonUtils.genOutputSql(dataset, options);
            dataset = sparkSession.sql(sql);

            String tdlName = "tdl_datax_" + System.currentTimeMillis();
            dataset.createTempView(tdlName);

            String databaseName = options.get("databaseName");
            String tableName = options.get("tableName");
            String partition = options.get("partition");
            String writeMode = options.get("writeMode");

            boolean isPartition = HiveUtils.checkPartition(sparkSession, databaseName, tableName);
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

            sparkSession.sql(sql);
        } catch (Exception e) {
            throw new DataTunnelException(e.getMessage(), e);
        }
    }
}
