package com.superior.datatunnel.plugin.redshift;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SnowflakeDataTunnelSink implements DataTunnelSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeDataTunnelSink.class);

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        SparkSession sparkSession = context.getSparkSession();
        SnowflakeDataTunnelSinkOption option = (SnowflakeDataTunnelSinkOption) context.getSinkOption();

        DataFrameWriter dataFrameWriter = dataset.write()
                .format("snowflake")
                .options(option.getParams())
                .option("sfURL", option.getHost())
                .option("sfUser", option.getUsername())
                .option("sfPassword", option.getPassword())
                .option("sfDatabase", option.getDatabaseName())
                .option("sfSchema", option.getSchemaName());

        if (StringUtils.isNotBlank(option.getWarehouse())) {
            dataFrameWriter.option("sfWarehouse", option.getWarehouse());
        }
        dataFrameWriter.option("dbtable", option.getTableName());
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return SnowflakeDataTunnelSinkOption.class;
    }
}
