package com.superior.datatunnel.plugin.file;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import com.superior.datatunnel.common.enums.FileFormat;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class FileDataTunnelSource implements DataTunnelSource {

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        FileDataTunnelSourceOption sourceOption = (FileDataTunnelSourceOption) context.getSourceOption();

        String format = sourceOption.getFormat().name().toLowerCase();
        if (FileFormat.EXCEL == sourceOption.getFormat()) {
            format = "com.crealytics.spark.excel";
        }

        SparkSession sparkSession = context.getSparkSession();
        DataFrameReader reader = sparkSession.read().format(format);
        reader.options(sourceOption.getProperties());

        if ("csv".equalsIgnoreCase(format)) {
            reader.option("sep", sourceOption.getSep());
            reader.option("encoding", sourceOption.getEncoding());
            reader.option("header", sourceOption.isHeader());
        }
        return reader.load(sourceOption.getPaths());
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return FileDataTunnelSourceOption.class;
    }
}
