package com.superior.datatunnel.distcp;

import com.superior.datatunnel.api.DistCpContext;
import com.superior.datatunnel.api.DistCpSink;
import com.superior.datatunnel.api.model.DistCpBaseSinkOption;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

public class HdfsDistCpSink implements DistCpSink {

    @Override
    public void sink(Dataset<Row> dataset, DistCpContext context) throws IOException {
        HdfsDistCpSinkOption sinkOption = (HdfsDistCpSinkOption) context.getSinkOption();
    }

    @Override
    public Class<? extends DistCpBaseSinkOption> getOptionClass() {
        return HdfsDistCpSinkOption.class;
    }
}
