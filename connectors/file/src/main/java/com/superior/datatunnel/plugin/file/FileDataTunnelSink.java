package com.superior.datatunnel.plugin.file;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.common.enums.FileFormat;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.Locale;

public class FileDataTunnelSink implements DataTunnelSink {

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        FileDataTunnelSinkOption sinkOption = (FileDataTunnelSinkOption) context.getSinkOption();

        String format = sinkOption.getFormat().name().toLowerCase();
        if (FileFormat.EXCEL == sinkOption.getFormat()) {
            format = "com.crealytics.spark.excel";
        }
        DataFrameWriter writer = dataset.write().format(format);
        writer.options(sinkOption.getProperties());
        if ("csv".equalsIgnoreCase(format)) {
            writer.option("sep", sinkOption.getSep());
            writer.option("encoding", sinkOption.getEncoding());
            writer.option("header", sinkOption.isHeader());
        }
        writer.option("compression", sinkOption.getCompression().name().toLowerCase(Locale.ROOT));
        writer.save(sinkOption.getPath());
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return FileDataTunnelSinkOption.class;
    }
}
