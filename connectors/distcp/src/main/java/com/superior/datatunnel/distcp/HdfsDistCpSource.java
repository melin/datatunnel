package com.superior.datatunnel.distcp;

import com.superior.datatunnel.api.DistCpContext;
import com.superior.datatunnel.api.DistCpSource;
import com.superior.datatunnel.api.model.DistCpBaseSourceOption;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.net.URISyntaxException;

public class HdfsDistCpSource implements DistCpSource {

    @Override
    public Dataset<Row> read(DistCpContext context) throws IOException, URISyntaxException {
        DistCpBaseSourceOption sourceOption = (DistCpBaseSourceOption) context.getSourceOption();
        return null;
    }

    @Override
    public Class<? extends DistCpBaseSourceOption> getOptionClass() {
        return HdfsDistCpSourceOption.class;
    }
}
