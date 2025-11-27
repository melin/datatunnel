package com.superior.datatunnel.plugin.oceanbase;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import java.io.IOException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class OceanBaseDataTunnelSource implements DataTunnelSource {

    private static final Logger logger = LoggerFactory.getLogger(OceanBaseDataTunnelSource.class);

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        SparkSession sparkSession = context.getSparkSession();
        OceanBaseDataTunnelSourceOption option = (OceanBaseDataTunnelSourceOption) context.getSourceOption();

        return sparkSession
                .read()
                .format("oceanbase")
                .options(option.getProperties())
                .option("url", option.getJdbcUrl())
                .option("username", option.getUsername())
                .option("password", option.getPassword())
                .option("table-name", option.getTableName())
                .option("schema-name", option.getSchemaName())
                .load();
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return OceanBaseDataTunnelSourceOption.class;
    }
}
