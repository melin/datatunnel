package com.superior.datatunnel.plugin.redshift;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class SnowflakeDataTunnelSource implements DataTunnelSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeDataTunnelSource.class);

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        SparkSession sparkSession = context.getSparkSession();
        SnowflakeDataTunnelSourceOption option = (SnowflakeDataTunnelSourceOption) context.getSourceOption();

        DataFrameReader reader = sparkSession
                .read()
                .format("snowflake")
                .options(option.getParams())
                .option("sfURL", option.getHost())
                .option("sfUser", option.getUsername())
                .option("sfPassword", option.getPassword())
                .option("sfDatabase", option.getDatabaseName())
                .option("sfSchema", option.getSchemaName());

        if (StringUtils.isNotBlank(option.getWarehouse())) {
            reader.option("sfWarehouse", option.getWarehouse());
        }

        if (StringUtils.isNotBlank(option.getQuery())) {
            reader.option("query", option.getQuery());
        } else {
            if (StringUtils.isBlank(option.getSchemaName())) {
                throw new DataTunnelException("schemaName can not blank");
            }
            if (StringUtils.isBlank(option.getTableName())) {
                throw new DataTunnelException("tableName can not blank");
            }

            reader.option("dbtable", option.getTableName());
        }

        return reader.load();
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return SnowflakeDataTunnelSourceOption.class;
    }
}
