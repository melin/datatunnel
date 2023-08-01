package com.superior.datatunnel.plugin.doris;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

public class DorisDataTunnelSource implements DataTunnelSource {

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        DorisDataTunnelSourceOption sourceOption = (DorisDataTunnelSourceOption) context.getSourceOption();

        DataFrameReader reader = context.getSparkSession().read().format("doris")
                .option("doris.fenodes", sourceOption.getFenodes())
                .option("user", sourceOption.getUser())
                .option("password", sourceOption.getPassword())
                .option("doris.table.identifier", sourceOption.getTableName())
                .option("doris.request.retries", sourceOption.getRetries())
                .option("doris.request.connect.timeout.ms", sourceOption.getConnectTimeout())
                .option("doris.request.read.timeout.ms", sourceOption.getReadTimeout())
                .option("doris.request.query.timeout.s", sourceOption.getQueryTimeout())
                .option("doris.request.tablet.size", sourceOption.getTabletSize())
                .option("doris.read.field", sourceOption.getColumns())
                .option("doris.batch.size", sourceOption.getBatchSize())
                .option("doris.exec.mem.limit", sourceOption.getMemLimit())
                .option("doris.deserialize.arrow.async", sourceOption.isDeserializeArrowAsync())
                .option("doris.deserialize.queue.size", sourceOption.getDeserializeQueueSize())
                .option("doris.filter.query.in.max.count", sourceOption.getFilterQueryInMaxCount());

        Dataset<Row> dataset = reader.load();
        try {
            String tdlName = "tdl_datatunnel_" + System.currentTimeMillis();
            dataset.createTempView(tdlName);
            String columns = sourceOption.getColumns();
            String sql = "select " + columns + " from " + tdlName;
            return context.getSparkSession().sql(sql);
        } catch (AnalysisException e) {
            throw new DataTunnelException(e.message(), e);
        }
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return DorisDataTunnelSourceOption.class;
    }
}
