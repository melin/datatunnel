package com.superior.datatunnel.plugin.hdfs;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.common.enums.FileFormat;
import com.superior.datatunnel.common.enums.WriteMode;
import java.io.IOException;
import java.util.Locale;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsDataTunnelSink implements DataTunnelSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsDataTunnelSink.class);

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        HdfsDataTunnelSinkOption sinkOption = (HdfsDataTunnelSinkOption) context.getSinkOption();

        String format = sinkOption.getFormat().name().toLowerCase();
        if (FileFormat.EXCEL == sinkOption.getFormat()) {
            format = "com.crealytics.spark.excel";
        }
        if (sinkOption.getFileCount() != null) {
            dataset = dataset.coalesce(sinkOption.getFileCount());
        }

        DataFrameWriter writer = dataset.write().format(format);

        if ("csv".equalsIgnoreCase(format)) {
            writer.option("sep", sinkOption.getSep());
            writer.option("encoding", sinkOption.getEncoding());
            writer.option("header", sinkOption.isHeader());
        }
        if (StringUtils.isNotBlank(sinkOption.getLineSep())) {
            writer.option("lineSep", sinkOption.getLineSep());
        }

        writer.option("timestampFormat", sinkOption.getTimestampFormat());
        writer.option("compression", sinkOption.getCompression().name().toLowerCase(Locale.ROOT));

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
