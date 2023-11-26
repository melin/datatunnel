package com.superior.datatunnel.plugin.starrocks;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author melin 2021/7/27 11:06 上午O
 */
public class StarrocksDataTunnelSource implements DataTunnelSource {

    private static final Logger LOG = LoggerFactory.getLogger(StarrocksDataTunnelSource.class);

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        StarrocksDataTunnelSourceOption sourceOption = (StarrocksDataTunnelSourceOption) context.getSourceOption();

        String jdbcUrl = sourceOption.getJdbcUrl();
        if (StringUtils.isBlank(jdbcUrl)) {
            if (StringUtils.isNotBlank(sourceOption.getHost())
                    && sourceOption.getPort() != null) {
                jdbcUrl = "jdbc:mysql://" + sourceOption.getHost() + ":" + sourceOption.getPort() + "/";
            } else {
                throw new IllegalArgumentException("Starrocks 不正确，添加jdbcUrl 或者 host & port");
            }
        }

        String databaseName = sourceOption.getDatabaseName();
        if (StringUtils.isBlank(databaseName)) {
            databaseName = sourceOption.getSchemaName();
        }

        if (StringUtils.isBlank(databaseName)) {
            throw new IllegalArgumentException("databaseName can not blank");
        }
        String fullTableId = databaseName + "." + sourceOption.getTableName();
        DataFrameReader reader = context.getSparkSession().read().format("starrocks")
                .options(sourceOption.getProperties())
                .option("starrocks.fe.http.url", sourceOption.getFeEnpoints())
                .option("starrocks.fe.jdbc.url", jdbcUrl)
                .option("starrocks.table.identifier", fullTableId)
                .option("starrocks.user", sourceOption.getUsername())
                .option("starrocks.password", sourceOption.getPassword());

        Dataset<Row> dataset = reader.load();
        try {
            String tdlName = "tdl_datatunnel_" + System.currentTimeMillis();
            dataset.createTempView(tdlName);
            String columns = StringUtils.join(sourceOption.getColumns(), ", ");
            String sql = "select " + columns + " from " + tdlName;
            return context.getSparkSession().sql(sql);
        } catch (AnalysisException e) {
            throw new DataTunnelException(e.message(), e);
        }
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return StarrocksDataTunnelSourceOption.class;
    }
}
