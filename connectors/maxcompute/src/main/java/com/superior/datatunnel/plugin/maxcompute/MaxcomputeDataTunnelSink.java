package com.superior.datatunnel.plugin.maxcompute;

import com.superior.datatunnel.api.DataSourceType;
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
public class MaxcomputeDataTunnelSink implements DataTunnelSink {

    private static final Logger LOG = LoggerFactory.getLogger(MaxcomputeDataTunnelSink.class);

    public void validateOptions(DataTunnelContext context) {
        DataSourceType dsType = context.getSinkOption().getDataSourceType();

        if (!DataSourceType.isJdbcDataSource(dsType)) {
            throw new IllegalArgumentException("不支持数据源类型: " + dsType);
        }
    }

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        validateOptions(context);

        MaxcomputeDataTunnelSinkOption sinkOption = (MaxcomputeDataTunnelSinkOption) context.getSinkOption();
        DataSourceType dataSourceType = sinkOption.getDataSourceType();
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return MaxcomputeDataTunnelSinkOption.class;
    }
}
