package com.superior.datatunnel.plugin.maxcompute;

import com.superior.datatunnel.api.DataSourceType;
import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author melin 2021/7/27 11:06 上午O
 */
public class MaxcomputeDataTunnelSource implements DataTunnelSource {

    private static final Logger LOG = LoggerFactory.getLogger(MaxcomputeDataTunnelSource.class);

    public void validateOptions(DataTunnelContext context) {
        DataSourceType dsType = context.getSourceOption().getDataSourceType();

        if (!DataSourceType.isJdbcDataSource(dsType)) {
            throw new IllegalArgumentException("不支持数据源类型: " + dsType);
        }
    }

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        return null;
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return MaxcomputeDataTunnelSourceOption.class;
    }
}
