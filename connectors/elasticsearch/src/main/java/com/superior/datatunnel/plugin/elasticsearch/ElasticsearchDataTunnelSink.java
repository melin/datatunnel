package com.superior.datatunnel.plugin.elasticsearch;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.elasticsearch.spark.sql.EsSparkSQL;
import scala.collection.JavaConverters;

public class ElasticsearchDataTunnelSink implements DataTunnelSink {

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        ElasticsearchDataTunnelSinkOption sinkOption = (ElasticsearchDataTunnelSinkOption) context.getSinkOption();
        String resource = sinkOption.getResource();
        Map<String, String> esCfg = new HashMap<>();
        esCfg.putAll(context.getSinkOption().getProperties());
        esCfg.put("es.nodes", sinkOption.getNodes());
        esCfg.put("es.port", sinkOption.getPort().toString());
        if (StringUtils.isNotBlank(sinkOption.getIndexKey())) {
            esCfg.put("es.mapping.id", sinkOption.getIndexKey());
        }

        EsSparkSQL.saveToEs(dataset, resource, JavaConverters.mapAsScalaMap(esCfg));
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return ElasticsearchDataTunnelSinkOption.class;
    }
}
