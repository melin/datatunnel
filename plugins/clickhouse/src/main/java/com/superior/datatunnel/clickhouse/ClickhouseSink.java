package com.superior.datatunnel.clickhouse;

import com.superior.datatunnel.api.DataTunnelSinkContext;
import com.superior.datatunnel.api.DataTunnelSink;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class ClickhouseSink implements DataTunnelSink<ClickhouseSinkOption> {

    private static final Logger logger = LoggerFactory.getLogger(ClickhouseSink.class);

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelSinkContext<ClickhouseSinkOption> context) throws IOException {

    }
}
