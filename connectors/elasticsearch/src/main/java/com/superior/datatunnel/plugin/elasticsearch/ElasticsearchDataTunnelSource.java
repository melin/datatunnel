package com.superior.datatunnel.plugin.elasticsearch;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class ElasticsearchDataTunnelSource implements DataTunnelSource {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchDataTunnelSource.class);

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        SparkSession sparkSession = context.getSparkSession();
        ElasticsearchDataTunnelSourceOption option = (ElasticsearchDataTunnelSourceOption) context.getSourceOption();

        Map<String, String> esCfg = option.getParams();
        esCfg.put("es.nodes", option.getNodes());
        esCfg.put("es.port", option.getPort().toString());

        if (StringUtils.isNotBlank(option.getQuery())) {
            esCfg.put("es.query", option.getQuery());
        }
        return sparkSession.read()
                .format("org.elasticsearch.spark.sql")
                .options(esCfg)
                .load(option.getResource());
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return ElasticsearchDataTunnelSourceOption.class;
    }
}
