package com.superior.datatunnel.plugin.maxcompute;

import com.clearspring.analytics.util.Lists;
import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.common.enums.WriteMode;
import com.superior.datatunnel.common.util.CommonUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.sql.functions;

import java.io.IOException;
import java.util.List;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class MaxcomputeDataTunnelSink implements DataTunnelSink {

    private static final Logger LOG = LoggerFactory.getLogger(MaxcomputeDataTunnelSink.class);

    private static final String ODPS_DATA_SOURCE = "org.apache.spark.sql.odps.datasource.DefaultSource";

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        MaxcomputeDataTunnelSinkOption sinkOption = (MaxcomputeDataTunnelSinkOption) context.getSinkOption();
        WriteMode writeMode = sinkOption.getWriteMode();

        if (WriteMode.UPSERT == writeMode) {
            throw new DataTunnelException("不支持的写入模式：" + writeMode);
        }

        String projectName = sinkOption.getProjectName();
        if (StringUtils.isBlank(projectName)) {
            projectName = sinkOption.getSchemaName();
        }
        if (StringUtils.isBlank(projectName)) {
            throw new IllegalArgumentException("projectName can not blank");
        }

        // 静态分区字段添加到df 上，否则mc 提示缺少字段
        if (StringUtils.isNotBlank(sinkOption.getPartitionSpec())) {
            String partitionSpec = sinkOption.getPartitionSpec();
            String[] parts = StringUtils.split(partitionSpec, ",");
            String[] columns = sinkOption.getColumns();
            for (String part : parts) {
                String[] items = StringUtils.split(part, "=");
                String columnName = items[0];
                if (!ArrayUtils.contains(columns, columnName)) {
                    String value = CommonUtils.cleanQuote(items[1]);
                    dataset = dataset.withColumn(columnName, functions.lit(value));
                }
            }
        }

        DataFrameWriter dataFrameWriter = dataset.write()
                .format(ODPS_DATA_SOURCE)
                .option("spark.hadoop.odps.project.name", projectName)
                .option("spark.hadoop.odps.access.id", sinkOption.getAccessKeyId())
                .option("spark.hadoop.odps.access.key", sinkOption.getSecretAccessKey())
                .option("spark.hadoop.odps.end.point", sinkOption.getEndpoint())
                .option("spark.hadoop.odps.table.name", sinkOption.getTableName())
                .option("spark.sql.odps.dynamic.partition", false);

        // spark.sql.odps.partition.spec 分区值不能有引号
        if (StringUtils.isNotBlank(sinkOption.getPartitionSpec())) {
            String partitionSpec = sinkOption.getPartitionSpec();
            String[] parts = StringUtils.split(partitionSpec, ",");
            List<String> list = Lists.newArrayList();
            for (int i = 0; i < parts.length; i++) {
                String[] items = StringUtils.split(parts[i], "=");
                String columnName = items[0];
                String value = CommonUtils.cleanQuote(items[1]);
                list.add(columnName + "=" + value);
            }
            dataFrameWriter.option("spark.sql.odps.partition.spec", StringUtils.join(list, ","));
        }
        dataFrameWriter.mode(writeMode == WriteMode.APPEND ? SaveMode.Append : SaveMode.Overwrite);
        dataFrameWriter.save();
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return MaxcomputeDataTunnelSinkOption.class;
    }
}
