package com.superior.datatunnel.plugin.opensearch;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class OpenSearchDataTunnelSink implements DataTunnelSink {

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        OpenSearchDataTunnelSinkOption sinkOption = (OpenSearchDataTunnelSinkOption) context.getSinkOption();
        Map<String, String> cfg = new HashMap<>();
        cfg.putAll(context.getSinkOption().getProperties());
        cfg.put("es.nodes", sinkOption.getNodes());
        cfg.put("es.port", sinkOption.getPort().toString());
        if (StringUtils.isNotBlank(sinkOption.getIndexId())) {
            cfg.put("es.mapping.id", sinkOption.getIndexId());
        }

        dataset.write().format("opensearch").options(cfg).save(sinkOption.getIndex());
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return OpenSearchDataTunnelSinkOption.class;
    }
}
