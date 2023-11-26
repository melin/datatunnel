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

        String databaseName = sinkOption.getDatabaseName();
        if (StringUtils.isBlank(databaseName)) {
            databaseName = sinkOption.getSchemaName();
        }

        if (StringUtils.isBlank(databaseName)) {
            throw new IllegalArgumentException("databaseName can not blank");
        }

        String fullTableId = databaseName + "." + sinkOption.getTableName();
        DataFrameWriter dataFrameWriter = dataset.write().format("doris")
                .options(sinkOption.getProperties())
                .option("doris.fenodes", sinkOption.getFeEnpoints())
                .option("user", sinkOption.getUsername())
                .option("password", sinkOption.getPassword())
                .option("doris.table.identifier", fullTableId);

        String[] columns = sinkOption.getColumns();
        if (!(ArrayUtils.isEmpty(columns) || (columns.length == 1 && "*".equals(columns[0])))) {
            dataFrameWriter.option("doris.write.fields", StringUtils.join(columns, ","));
        }

        dataFrameWriter.mode(SaveMode.Append);
        dataFrameWriter.save();
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return DorisDataTunnelSinkOption.class;
    }
}
