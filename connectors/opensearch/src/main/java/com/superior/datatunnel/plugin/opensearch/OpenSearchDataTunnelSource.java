package com.superior.datatunnel.plugin.opensearch;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class OpenSearchDataTunnelSource implements DataTunnelSource {

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchDataTunnelSource.class);

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        SparkSession sparkSession = context.getSparkSession();
        OpenSearchDataTunnelSourceOption option = (OpenSearchDataTunnelSourceOption) context.getSourceOption();

        Map<String, String> cfg = new HashMap<>();
        cfg.putAll(context.getSourceOption().getProperties());
        cfg.put("opensearch.nodes", option.getNodes());
        cfg.put("opensearch.port", option.getPort().toString());

        if (StringUtils.isNotBlank(option.getQuery())) {
            cfg.put("opensearch.query", option.getQuery());
        }
        return sparkSession.read().format("opensearch").options(cfg).load(option.getIndex());
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return OpenSearchDataTunnelSourceOption.class;
    }
}
