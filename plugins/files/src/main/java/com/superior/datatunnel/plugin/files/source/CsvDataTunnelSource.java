package com.superior.datatunnel.plugin.files.source;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import com.superior.datatunnel.common.util.ReflectionUtils;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

public class CsvDataTunnelSource implements DataTunnelSource {

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        CsvDataTunnelSourceOption option = (CsvDataTunnelSourceOption) context.getSourceOption();
        DataFrameReader reader = context.getSparkSession().read();

        ReflectionUtils.setDataFrameReaderOptions(reader, option);
        return reader.csv(option.getPath());
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return CsvDataTunnelSourceOption.class;
    }
}
