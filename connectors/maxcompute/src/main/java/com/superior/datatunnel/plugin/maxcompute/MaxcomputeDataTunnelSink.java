package com.superior.datatunnel.plugin.maxcompute;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.common.enums.WriteMode;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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

        DataFrameWriter dataFrameWriter = dataset.write()
                .format(ODPS_DATA_SOURCE)
                .option("spark.hadoop.odps.project.name", projectName)
                .option("spark.hadoop.odps.access.id", sinkOption.getAccessKeyId())
                .option("spark.hadoop.odps.access.key", sinkOption.getSecretAccessKey())
                .option("spark.hadoop.odps.end.point", sinkOption.getEndpoint())
                .option("spark.hadoop.odps.table.name", sinkOption.getTableName())
                .option("spark.sql.odps.dynamic.partition", true);

        if (StringUtils.isNotBlank(sinkOption.getPartitionSpec())) {
            dataFrameWriter.option("spark.sql.odps.partition.spec", sinkOption.getPartitionSpec());
        }
        dataFrameWriter.mode(writeMode == WriteMode.APPEND ? SaveMode.Append : SaveMode.Overwrite);
        dataFrameWriter.save();
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return MaxcomputeDataTunnelSinkOption.class;
    }
}
