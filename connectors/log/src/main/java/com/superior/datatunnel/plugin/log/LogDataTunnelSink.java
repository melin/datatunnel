package com.superior.datatunnel.plugin.log;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSink;
import io.github.melin.jobserver.spark.api.LogUtils;
import java.io.IOException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class LogDataTunnelSink implements DataTunnelSink {

    private static final Logger LOG = LoggerFactory.getLogger(LogDataTunnelSink.class);

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        LogDataTunnelSinkOption sinkOption = (LogDataTunnelSinkOption) context.getSinkOption();
        int numRows = sinkOption.getNumRows();
        int truncate = sinkOption.getTruncate();
        boolean vertical = sinkOption.isVertical();
        String data = dataset.showString(numRows, truncate, vertical);
        LogUtils.stdout(data);
        LOG.info("log sink result:\n" + data);
    }

    @Override
    public Class<LogDataTunnelSinkOption> getOptionClass() {
        return LogDataTunnelSinkOption.class;
    }
}
