package com.superior.datatunnel.log;

import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.DataTunnelSinkContext;
import com.github.melin.superior.jobserver.api.LogUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class LogSink implements DataTunnelSink<LogSinkOption> {

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelSinkContext<LogSinkOption> context) throws IOException {
        LogSinkOption sinkOption = context.getSinkOption();
        int numRows = sinkOption.getNumRows();
        int truncate = sinkOption.getTruncate();
        boolean vertical = sinkOption.isVertical();
        String data = dataset.showString(numRows, truncate, vertical);
        LogUtils.stdout(data);
    }
}
