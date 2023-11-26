package com.superior.datatunnel.plugin.doris;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

public class DorisDataTunnelSource implements DataTunnelSource {

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        DorisDataTunnelSourceOption sourceOption = (DorisDataTunnelSourceOption) context.getSourceOption();

        String databaseName = sourceOption.getDatabaseName();
        if (StringUtils.isBlank(databaseName)) {
            databaseName = sourceOption.getSchemaName();
        }

        if (StringUtils.isBlank(databaseName)) {
            throw new IllegalArgumentException("databaseName can not blank");
        }

        String fullTableId = databaseName + "." + sourceOption.getTableName();
        DataFrameReader reader = context.getSparkSession().read().format("doris")
                .options(sourceOption.getProperties())
                .option("doris.fenodes", sourceOption.getFeEnpoints())
                .option("user", sourceOption.getUsername())
                .option("password", sourceOption.getPassword())
                .option("doris.table.identifier", fullTableId);

        String[] columns = sourceOption.getColumns();
        if (!(ArrayUtils.isEmpty(columns) || (columns.length == 1 && "*".equals(columns[0])))) {
            reader.option("doris.read.field", StringUtils.join(columns, ","));
        }

        return reader.load();
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return DorisDataTunnelSourceOption.class;
    }
}
