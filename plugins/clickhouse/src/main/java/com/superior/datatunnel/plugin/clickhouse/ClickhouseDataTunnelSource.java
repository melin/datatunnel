package com.superior.datatunnel.plugin.clickhouse;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import com.superior.datatunnel.common.util.CommonUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xenon.clickhouse.ClickHouseCatalog;

import java.io.IOException;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class ClickhouseDataTunnelSource implements DataTunnelSource {

    private static final Logger logger = LoggerFactory.getLogger(ClickhouseDataTunnelSource.class);

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        SparkSession sparkSession = context.getSparkSession();
        ClickhouseDataTunnelSourceOption option = (ClickhouseDataTunnelSourceOption) context.getSourceOption();
        sparkSession.conf().set("spark.sql.catalog.clickhouse", ClickHouseCatalog.class.getName());
        sparkSession.conf().set("spark.sql.catalog.clickhouse.protocol", option.getProtocol());
        if ("http".equals(option.getProtocol())) {
            sparkSession.conf().set("spark.sql.catalog.clickhouse.http_port", option.getPort());
        } else {
            sparkSession.conf().set("spark.sql.catalog.clickhouse.grpc_port", option.getPort());
        }
        sparkSession.conf().set("spark.sql.catalog.clickhouse.database", "default");
        CommonUtils.convertOptionToSparkConf(sparkSession, option);

        try {
            String ckTableName = "clickhouse." + option.getDatabaseName() + "." + option.getTableName();
            String sql = "select " + StringUtils.join(option.getColumns(), ", ") + " from " + ckTableName;
            String condition = option.getCondition();
            if (StringUtils.isNotBlank(condition)) {
                sql = sql + " where " + condition;
            }
            return sparkSession.sql(sql);
        } catch (Exception e) {
            throw new DataTunnelException(e.getMessage(), e);
        }
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return ClickhouseDataTunnelSourceOption.class;
    }
}
