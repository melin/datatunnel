package com.superior.datatunnel.sftp;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class SftpDataTunnelSource implements DataTunnelSource {

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        SftpDataTunnelSourceOption sourceOption = (SftpDataTunnelSourceOption) context.getSourceOption();
        DataFrameReader dfReader = context.getSparkSession().read()
                .format("com.superior.datatunnel.sftp.spark");
        String path = sourceOption.getPath();
        dfReader.options(context.getSourceOption().getParams());
        return dfReader.load(path);
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return SftpDataTunnelSourceOption.class;
    }
}
