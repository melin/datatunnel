package com.superior.datatunnel.plugin.cassandra;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class CassandraDataTunnelSink implements DataTunnelSink {

    private static final Logger logger = LoggerFactory.getLogger(CassandraDataTunnelSink.class);

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return CassandraDataTunnelSinkOption.class;
    }
}
