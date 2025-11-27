package com.superior.datatunnel.plugin.oceanbase;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import java.io.IOException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OceanBaseDataTunnelSink implements DataTunnelSink {

    private static final Logger logger = LoggerFactory.getLogger(OceanBaseDataTunnelSink.class);

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        OceanBaseDataTunnelSinkOption option = (OceanBaseDataTunnelSinkOption) context.getSinkOption();

        dataset.write()
                .format("oceanbase")
                .mode(SaveMode.Append)
                .options(option.getProperties())
                .option("url", option.getJdbcUrl())
                .option("username", option.getUsername())
                .option("password", option.getPassword())
                .option("table-name", option.getTableName())
                .option("schema-name", option.getSchemaName())
                .save();
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return OceanBaseDataTunnelSinkOption.class;
    }
}
