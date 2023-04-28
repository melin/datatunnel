package com.superior.datatunnel.plugin.files;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.common.enums.FileFormat;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

public class FileDataTunnelSink implements DataTunnelSink {

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        FileDataTunnelSinkOption sinkOption = (FileDataTunnelSinkOption) context.getSinkOption();

        String format = sinkOption.getFormat().name().toLowerCase();
        if (FileFormat.EXCEL == sinkOption.getFormat()) {
            format = "com.crealytics.spark.excel";
        }
        DataFrameWriter writer = dataset.write().format(format);

        sinkOption.getProperties().forEach(writer::option);
        writer.save(sinkOption.getFilePath());
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return FileDataTunnelSinkOption.class;
    }
}
