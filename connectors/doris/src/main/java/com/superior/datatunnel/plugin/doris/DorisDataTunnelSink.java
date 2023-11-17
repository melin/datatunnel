package com.superior.datatunnel.plugin.doris;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.io.IOException;

public class DorisDataTunnelSink implements DataTunnelSink {

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        DorisDataTunnelSinkOption sinkOption = (DorisDataTunnelSinkOption) context.getSinkOption();

        DataFrameWriter dataFrameWriter = dataset.write().format("doris")
                .option("doris.fenodes", sinkOption.getFenodes())
                .option("user", sinkOption.getUser())
                .option("password", sinkOption.getPassword())
                .option("doris.table.identifier", sinkOption.getTableName())

                .option("sink.batch.size", sinkOption.getBatchSize())
                .option("doris.sink.task.use.repartition", sinkOption.isRepartition())
                .option("doris.sink.batch.interval.ms", sinkOption.getIntervalTimes())
                .option("doris.ignore-type", sinkOption.getIgnoreType());

        String[] columns = sinkOption.getColumns();
        if (!(ArrayUtils.isEmpty(columns) || (columns.length == 1 && "*".equals(columns[0])))) {
            dataFrameWriter.option("doris.write.fields", StringUtils.join(columns, ","));
        }

        if (sinkOption.getPartitionSize() != null) {
            dataFrameWriter.option("doris.sink.task.partition.size", sinkOption.getPartitionSize());
        }

        sinkOption.getProperties().forEach((key, value) -> {
            dataFrameWriter.option("sink.properties." + key, value);
        });

        dataFrameWriter.mode(SaveMode.Append);
        dataFrameWriter.save();
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return DorisDataTunnelSinkOption.class;
    }
}
