package com.superior.datatunnel.plugin.clickhouse;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xenon.clickhouse.ClickHouseCatalog;

public class ClickhouseDataTunnelSink implements DataTunnelSink {

    private static final Logger logger = LoggerFactory.getLogger(ClickhouseDataTunnelSink.class);

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        SparkSession sparkSession = context.getSparkSession();
        ClickhouseDataTunnelSinkOption option = (ClickhouseDataTunnelSinkOption) context.getSinkOption();
        sparkSession.conf().set("spark.sql.catalog.datatunnel_clickhouse", ClickHouseCatalog.class.getName());
        sparkSession.conf().set("spark.sql.catalog.datatunnel_clickhouse.protocol", option.getProtocol());

        String host = option.getHost();
        Integer port = option.getPort();
        String protocol = option.getProtocol();
        String jdbcUrl = option.getJdbcUrl();
        if (StringUtils.isNotBlank(jdbcUrl)) {
            if (StringUtils.contains(jdbcUrl, "https://")) {
                sparkSession.conf().set("spark.sql.catalog.datatunnel_clickhouse.option.ssl", true);
                jdbcUrl = StringUtils.substringAfter(jdbcUrl, "https://");
                protocol = "https";
            } else if (StringUtils.contains(jdbcUrl, "grpc://")) {
                throw new DataTunnelException("not support protocol");
            } else {
                sparkSession.conf().set("spark.sql.catalog.datatunnel_clickhouse.option.ssl", false);
                jdbcUrl = StringUtils.substringAfter(jdbcUrl, "://");
                protocol = "http";
            }

            if (StringUtils.contains(jdbcUrl, "?")) {
                jdbcUrl = StringUtils.substringBefore(jdbcUrl, "?");
            }
            String[] items = StringUtils.split(jdbcUrl, ":");
            host = items[0];
            port = Integer.valueOf(items[1]);
        }

        sparkSession.conf().set("spark.sql.catalog.datatunnel_clickhouse.protocol", protocol);
        sparkSession.conf().set("spark.sql.catalog.datatunnel_clickhouse.host", host);
        sparkSession.conf().set("spark.sql.catalog.datatunnel_clickhouse.user", option.getUsername());
        sparkSession.conf().set("spark.sql.catalog.datatunnel_clickhouse.password", option.getPassword());
        sparkSession.conf().set("spark.sql.catalog.datatunnel_clickhouse.http_port", port);

        sparkSession.conf().set("spark.sql.catalog.datatunnel_clickhouse.database", "default");

        sparkSession.conf().set("spark.clickhouse.write.batchSize", option.getBatchsize());

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
