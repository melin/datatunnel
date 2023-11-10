package com.superior.datatunnel.plugin.redshift;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class RedshiftDataTunnelSource implements DataTunnelSource {

    private static final Logger logger = LoggerFactory.getLogger(RedshiftDataTunnelSource.class);

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        SparkSession sparkSession = context.getSparkSession();
        RedshiftDataTunnelSourceOption option = (RedshiftDataTunnelSourceOption) context.getSourceOption();

        return null;
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return RedshiftDataTunnelSourceOption.class;
    }
}
