package com.superior.datatunnel.plugin.log;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSink;
import io.github.melin.jobserver.spark.api.LogUtils;
import java.io.IOException;
import java.lang.reflect.Field;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class LogDataTunnelSink implements DataTunnelSink {

    private static final Logger LOG = LoggerFactory.getLogger(LogDataTunnelSink.class);

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        SparkSession sparkSession = context.getSparkSession();
        LogDataTunnelSinkOption sinkOption = (LogDataTunnelSinkOption) context.getSinkOption();
        int numRows = sinkOption.getNumRows();
        int truncate = sinkOption.getTruncate();
        boolean vertical = sinkOption.isVertical();

        // 支持 superior 平台显示log 结果 start
        String jobType = sparkSession.conf().get("spark.jobserver.superior.jobType", null);
        String instanceType = sparkSession.conf().get("spark.jobserver.superior.instanceType", null);
        if ("spark_sql".equals(jobType) && "dev".equals(instanceType)) {
            String instanceCode = sparkSession.conf().get("spark.jobserver.superior.instanceCode");
            try {
                Class<?> clazz = Class.forName("io.github.melin.jobserver.spark.driver.task.SparkSqlTask");
                Field field = clazz.getDeclaredField("sparkSqlTask");
                field.setAccessible(true);
                Object sparkSqlTask = field.get(null);

                MethodUtils.invokeMethod(
                        sparkSqlTask,
                        true,
                        "fetchResult",
                        context.getSql(),
                        dataset.limit(numRows),
                        instanceCode,
                        numRows);
                return;
            } catch (Throwable e) {
                LOG.error("load SparkSqlTask failed: " + e.getMessage());
            }
        }
        // 支持 superior 平台显示log 结果 end

        String data = dataset.showString(numRows, truncate, vertical);
        LogUtils.stdout(data);
        LOG.info("log sink result:\n" + data);
    }

    @Override
    public Class<LogDataTunnelSinkOption> getOptionClass() {
        return LogDataTunnelSinkOption.class;
    }
}
