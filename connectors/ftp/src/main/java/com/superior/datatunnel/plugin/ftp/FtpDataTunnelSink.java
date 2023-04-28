package com.superior.datatunnel.plugin.ftp;

import com.superior.datatunnel.api.*;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.superior.datatunnel.api.DataSourceType.HDFS;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class FtpDataTunnelSink implements DataTunnelSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(FtpDataTunnelSink.class);

    private void validateOptions(DataTunnelContext context) {
        DataSourceType dsType = context.getSourceOption().getDataSourceType();
        if (HDFS == dsType) {
            throw new DataTunnelException("只支持从hdfs读取文件写入sftp");
        }
    }

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        validateOptions(context);


    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return FtpDataTunnelSinkOption.class;
    }
}
