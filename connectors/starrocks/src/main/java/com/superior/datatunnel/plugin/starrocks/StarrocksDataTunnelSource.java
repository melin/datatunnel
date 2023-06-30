package com.superior.datatunnel.plugin.starrocks;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
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

        return null;
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return StarrocksDataTunnelSourceOption.class;
    }
}
