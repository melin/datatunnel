package com.superior.datatunnel.plugin.hdfs;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.common.enums.FileFormat;
import com.superior.datatunnel.common.enums.WriteMode;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.io.IOException;

public class HdfsDataTunnelSink implements DataTunnelSink {

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        HdfsDataTunnelSinkOption sinkOption = (HdfsDataTunnelSinkOption) context.getSinkOption();

        String format = sinkOption.getFormat().name().toLowerCase();
        if (FileFormat.EXCEL == sinkOption.getFormat()) {
            format = "com.crealytics.spark.excel";
        }
        DataFrameWriter writer = dataset.write().format(format);

        if ((FileFormat.ORC == sinkOption.getFormat() ||
                FileFormat.PARQUET == sinkOption.getFormat())) {
            writer.option("compression", sinkOption.getCompression().name().toLowerCase());
        }

        sinkOption.getProperties().forEach(writer::option);
        if (WriteMode.OVERWRITE == sinkOption.getWriteMode()) {
            writer.mode(SaveMode.Overwrite);
        } else {
            writer.mode(SaveMode.Append);
        }
        writer.save(sinkOption.getPath());
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return HdfsDataTunnelSinkOption.class;
    }
}
