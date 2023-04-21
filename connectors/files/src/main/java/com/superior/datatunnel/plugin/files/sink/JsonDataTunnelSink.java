package com.superior.datatunnel.plugin.files.sink;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.common.util.ReflectionUtils;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

public class JsonDataTunnelSink implements DataTunnelSink {

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        JsonDataTunnelSinkOption option = (JsonDataTunnelSinkOption) context.getSinkOption();
        DataFrameWriter<Row> writer = dataset.write();

        ReflectionUtils.setDataFrameWriterOptions(writer, option);
        writer.json(option.getPath());
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return JsonDataTunnelSinkOption.class;
    }
}
