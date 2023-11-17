package com.superior.datatunnel.plugin.starrocks;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class StarrocksDataTunnelSink implements DataTunnelSink {

    private static final Logger LOG = LoggerFactory.getLogger(StarrocksDataTunnelSink.class);

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        StarrocksDataTunnelSinkOption sinkOption = (StarrocksDataTunnelSinkOption) context.getSinkOption();

        DataFrameWriter dataFrameWriter = dataset.write().format("starrocks")
                .option("starrocks.fe.http.url", sinkOption.getFeHttpUrl())
                .option("starrocks.fe.jdbc.url", sinkOption.getFeJdbcUrl())
                .option("starrocks.table.identifier", sinkOption.getTableName())
                .option("starrocks.user", sinkOption.getUser())
                .option("starrocks.password", sinkOption.getPassword())
                .option("starrocks.write.label.prefix", sinkOption.getWriteLabelPrefix())
                .option("starrocks.write.enable.transaction-stream-load", sinkOption.getTransactionEnabled())
                .option("starrocks.write.buffer.size", sinkOption.getWriteBufferSize())
                .option("starrocks.write.flush.interval.ms", sinkOption.getWriteFlushInterval());

        String[] columns = sinkOption.getColumns();
        if (!(ArrayUtils.isEmpty(columns) || (columns.length == 1 && "*".equals(columns[0])))) {
            dataFrameWriter.option("starrocks.columns", StringUtils.join(columns, ","));
        }
        if (sinkOption.getWritePartitionNum() != null) {
            dataFrameWriter.option("starrocks.write.num.partitions", sinkOption.getWritePartitionNum());
        }
        if (StringUtils.isNotBlank(sinkOption.getWritePartitionColumns())) {
            dataFrameWriter.option("starrocks.write.partition.columns", sinkOption.getWritePartitionColumns());
        }

        sinkOption.getProperties().forEach((key, value) -> {
            dataFrameWriter.option("starrocks.write.properties." + key, value);
        });

        dataFrameWriter.mode(SaveMode.Append);
        dataFrameWriter.save();
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return StarrocksDataTunnelSinkOption.class;
    }
}
