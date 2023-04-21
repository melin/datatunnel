package com.superior.datatunnel.plugin.files.source;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

public class ExcelDataTunnelSource implements DataTunnelSource {

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        ExcelDataTunnelSourceOption option = (ExcelDataTunnelSourceOption) context.getSourceOption();
        DataFrameReader reader = context.getSparkSession().read()
                .format("com.crealytics.spark.excel")
                .option("dataAddress", option.getDataAddress())
                .option("header", option.isHeader())
                .option("treatEmptyValuesAsNulls", option.isTreatEmptyValuesAsNulls())
                .option("setErrorCellsToFallbackValues", option.isSetErrorCellsToFallbackValues())
                .option("usePlainNumberFormat", option.isUsePlainNumberFormat())
                .option("inferSchema", option.isInferSchema())
                .option("addColorColumns", option.isAddColorColumns())
                .option("timestampFormat", option.getTimestampFormat());

        if (option.getMaxRowsInMemory() > 0) {
            reader.option("maxRowsInMemory", option.getMaxRowsInMemory());
        }
        if (option.getMaxByteArraySize() > 0) {
            reader.option("maxByteArraySize", option.getMaxByteArraySize());
        }
        if (option.getTempFileThreshold() > 0) {
            reader.option("tempFileThreshold", option.getTempFileThreshold());
        }
        if (option.getExcerptSize() > 0) {
            reader.option("excerptSize", option.getExcerptSize());
        }
        if (StringUtils.isNotBlank(option.getWorkbookPassword())) {
            reader.option("workbookPassword", option.getWorkbookPassword());
        }

        if (StringUtils.isNotBlank(option.getSchema())) {
            reader.schema(option.getSchema());
        }
        return reader.load(option.getPath());
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return ExcelDataTunnelSourceOption.class;
    }
}
