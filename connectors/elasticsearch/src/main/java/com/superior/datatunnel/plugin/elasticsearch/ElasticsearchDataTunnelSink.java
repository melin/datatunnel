package com.superior.datatunnel.plugin.elasticsearch;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import java.io.IOException;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.elasticsearch.spark.sql.EsSparkSQL;
import scala.collection.JavaConverters;

public class ElasticsearchDataTunnelSink implements DataTunnelSink {

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        ElasticsearchDataTunnelSinkOption sinkOption = (ElasticsearchDataTunnelSinkOption) context.getSinkOption();
        String index = sinkOption.getResource();
        Map<String, String> esCfg = sinkOption.getParams();
        esCfg.put("es.nodes", sinkOption.getNodes());
        esCfg.put("es.port", sinkOption.getPort().toString());
        esCfg.put("es.mapping.id", sinkOption.getIndexKey());

        EsSparkSQL.saveToEs(dataset, index, JavaConverters.mapAsScalaMap(esCfg));
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return ElasticsearchDataTunnelSinkOption.class;
    }
}
