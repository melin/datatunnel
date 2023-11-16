package com.superior.datatunnel.plugin.redshift;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.common.enums.WriteMode;
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
                .options(option.getProperties())
                .option("sfURL", option.getHost())
                .option("sfUser", option.getUsername())
                .option("sfPassword", option.getPassword())
                .option("sfDatabase", option.getDatabaseName())
                .option("sfSchema", option.getSchemaName());

        if (StringUtils.isNotBlank(option.getWarehouse())) {
            dataFrameWriter.option("sfWarehouse", option.getWarehouse());
        }

        final WriteMode writeMode = option.getWriteMode();
        SaveMode mode = SaveMode.Append;
        if (WriteMode.OVERWRITE == writeMode) {
            mode = SaveMode.Overwrite;
        }
        dataFrameWriter.option("dbtable", option.getSchemaName() + "." + option.getTableName());
        dataFrameWriter.mode(mode).save();
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return SnowflakeDataTunnelSinkOption.class;
    }
}
