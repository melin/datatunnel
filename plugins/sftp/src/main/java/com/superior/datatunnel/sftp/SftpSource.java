package com.superior.datatunnel.sftp;

import com.superior.datatunnel.api.DataTunnelSourceContext;
import com.superior.datatunnel.api.DataTunnelSource;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class SftpSource implements DataTunnelSource<SftpSourceOption> {

    @Override
    public Dataset<Row> read(DataTunnelSourceContext<SftpSourceOption> context) throws IOException {
        DataFrameReader dfReader = context.getSparkSession().read()
                .format("com.superior.datatunnel.sftp.spark");
        String path = context.getSourceOption().getPath();
        dfReader.options(context.getSourceOption().getParams());
        return dfReader.load(path);
    }
}
