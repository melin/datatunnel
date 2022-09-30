package com.superior.datatunnel.plugin.tidb;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import com.superior.datatunnel.common.util.CommonUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.TiExtensions;
import org.apache.spark.sql.catalyst.catalog.TiCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class TidbDataTunnelSource implements DataTunnelSource {

    private static final Logger logger = LoggerFactory.getLogger(TidbDataTunnelSource.class);

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        SparkSession sparkSession = context.getSparkSession();
        TidbDataTunnelSourceOption option = (TidbDataTunnelSourceOption) context.getSourceOption();
        sparkSession.conf().set("spark.sql.extensions", TiExtensions.class.getName());
        sparkSession.conf().set("spark.tispark.pd.addresses", option.getPdAddress());
        sparkSession.conf().set("spark.sql.catalog.tidb_catalog", TiCatalog.class.getName());
        sparkSession.conf().set("spark.sql.catalog.tidb_catalog.pd.addresses", option.getPdAddress());
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
        return TidbDataTunnelSourceOption.class;
    }
}
