package com.superior.datatunnel.plugin.clickhouse;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.common.util.CommonUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xenon.clickhouse.ClickHouseCatalog;

import java.io.IOException;

public class ClickhouseDataTunnelSink implements DataTunnelSink {

    private static final Logger logger = LoggerFactory.getLogger(ClickhouseDataTunnelSink.class);

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        SparkSession sparkSession = context.getSparkSession();
        ClickhouseDataTunnelSinkOption option = (ClickhouseDataTunnelSinkOption) context.getSinkOption();
        sparkSession.conf().set("spark.sql.catalog.datatunnel_clickhouse", ClickHouseCatalog.class.getName());
        sparkSession.conf().set("spark.sql.catalog.datatunnel_clickhouse.protocol", option.getProtocol());
        if ("http".equals(option.getProtocol())) {
            sparkSession.conf().set("spark.sql.catalog.datatunnel_clickhouse.http_port", option.getPort());
        } else {
            sparkSession.conf().set("spark.sql.catalog.datatunnel_clickhouse.grpc_port", option.getPort());
        }
        sparkSession.conf().set("spark.sql.catalog.datatunnel_clickhouse.database", "default");
        CommonUtils.convertOptionToSparkConf(sparkSession, option);

        try {
            String tdlName = "tdl_datatunnel_" + System.currentTimeMillis();
            dataset.createTempView(tdlName);
            String ckTableName = "datatunnel_clickhouse." + option.getDatabaseName() + "." + option.getTableName();
            String sql = "insert into " + ckTableName + " select * from " + tdlName;
            sparkSession.sql(sql);
        } catch (Exception e) {
            throw new DataTunnelException(e.getMessage(), e);
        }
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return ClickhouseDataTunnelSinkOption.class;
    }
}
