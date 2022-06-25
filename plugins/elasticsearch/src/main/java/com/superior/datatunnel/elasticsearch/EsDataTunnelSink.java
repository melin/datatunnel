package com.superior.datatunnel.elasticsearch;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.elasticsearch.spark.sql.EsSparkSQL;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.Map;

public class EsDataTunnelSink implements DataTunnelSink {

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        EsDataTunnelSinkOption sinkOption = (EsDataTunnelSinkOption) context.getSinkOption();
        String index = sinkOption.getResource();
        Map<String, String> esCfg = sinkOption.getParams();
        EsSparkSQL.saveToEs(dataset, index, JavaConverters.mapAsScalaMap(esCfg));
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return EsDataTunnelSinkOption.class;
    }
}
