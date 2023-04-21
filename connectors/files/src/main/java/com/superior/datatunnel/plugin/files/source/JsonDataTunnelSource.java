package com.superior.datatunnel.plugin.files.source;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import com.superior.datatunnel.common.util.ReflectionUtils;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

public class JsonDataTunnelSource implements DataTunnelSource {

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        JsonDataTunnelSourceOption option = (JsonDataTunnelSourceOption) context.getSourceOption();
        DataFrameReader reader = context.getSparkSession().read()
                .format("com.crealytics.spark.excel");

        ReflectionUtils.setDataFrameReaderOptions(reader, option);
        return reader.json(option.getPath());
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return JsonDataTunnelSourceOption.class;
    }
}
