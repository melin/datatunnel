package com.superior.datatunnel.plugin.starrocks;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class StarrocksDataTunnelSink implements DataTunnelSink {

    private static final Logger LOG = LoggerFactory.getLogger(StarrocksDataTunnelSink.class);

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        StarrocksDataTunnelSinkOption sinkOption = (StarrocksDataTunnelSinkOption) context.getSinkOption();

        String jdbcUrl = sinkOption.getJdbcUrl();
        if (StringUtils.isBlank(jdbcUrl)) {
            if (StringUtils.isNotBlank(sinkOption.getHost())
                    && sinkOption.getPort() != null) {
                jdbcUrl = "jdbc:mysql://" + sinkOption.getHost() + ":" + sinkOption.getPort() + "/";
            } else {
                throw new IllegalArgumentException("Starrocks 不正确，添加jdbcUrl 或者 host & port");
            }
        }

        String fullTableId = sinkOption.getDatabaseName() + "." + sinkOption.getTableName();
        DataFrameWriter dataFrameWriter = dataset.write().format("starrocks")
                .options(sinkOption.getProperties())
                .option("starrocks.fe.http.url", sinkOption.getFeEnpoints())
                .option("starrocks.fe.jdbc.url", jdbcUrl)
                .option("starrocks.table.identifier", fullTableId)
                .option("starrocks.user", sinkOption.getUser())
                .option("starrocks.password", sinkOption.getPassword());

        String[] columns = sinkOption.getColumns();
        if (!(ArrayUtils.isEmpty(columns) || (columns.length == 1 && "*".equals(columns[0])))) {
            dataFrameWriter.option("starrocks.columns", StringUtils.join(columns, ","));
        }
        dataFrameWriter.mode(SaveMode.Append);
        dataFrameWriter.save();
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return StarrocksDataTunnelSinkOption.class;
    }
}
