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

        DataFrameReader reader = context.getSparkSession().read().format("starrocks")
                .option("starrocks.fe.http.url", sourceOption.getFeHttpUrl())
                .option("starrocks.fe.jdbc.url", sourceOption.getFeJdbcUrl())
                .option("starrocks.table.identifier", sourceOption.getTableName())
                .option("starrocks.user", sourceOption.getUser())
                .option("starrocks.password", sourceOption.getPassword())
                .option("starrocks.request.retries", sourceOption.getRetries())
                .option("starrocks.request.connect.timeout.ms", sourceOption.getConnectTimeout())
                .option("starrocks.request.read.timeout.ms", sourceOption.getReadTimeout())
                .option("starrocks.request.query.timeout.s", sourceOption.getQueryTimeout())
                .option("starrocks.request.tablet.size", sourceOption.getTableSize())
                .option("starrocks.batch.size", sourceOption.getBatchSize())
                .option("starrocks.exec.mem.limit", sourceOption.getMemLimit())
                .option("starrocks.deserialize.arrow.async", sourceOption.isArrowAsync())
                .option("starrocks.deserialize.queue.size", sourceOption.getDeserializeQueueSize())
                .option("starrocks.filter.query.in.max.count", sourceOption.getFilterQueryInMaxCount());

        if (StringUtils.isNotBlank(sourceOption.getFilterQuery())) {
            reader.option("starrocks.filter.query", sourceOption.getFilterQuery());
        }

        Dataset<Row> dataset = reader.load();
        try {
            String tdlName = "tdl_datatunnel_" + System.currentTimeMillis();
            dataset.createTempView(tdlName);
            String[] columns = sourceOption.getColumns();
            String sql = "select " + StringUtils.join(columns, ",") + " from " + tdlName;
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
