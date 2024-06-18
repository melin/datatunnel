package io.github.melin.jobserver.spark.api;

import org.apache.spark.scheduler.LiveListenerBus;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

/**
 * @author melin 2021/8/18 10:53 上午
 */
public class LogUtils {

    private static final Logger LOG = LoggerFactory.getLogger(LogUtils.class);

    public static void info(String format, Object... params) {
        String message = format;
        if (params.length > 0) {
            message = MessageFormatter.arrayFormat(format, params).getMessage();
        }

        LiveListenerBus listenerBus = SparkSession.active().sparkContext().listenerBus();
        JobServerLogEvent logEvent = new JobServerLogEvent(LogLevel.INFO, message);
        listenerBus.post(logEvent);
        LOG.info(format, params);
    }

    public static void warn(String format, Object... params) {
        String message = format;
        if (params.length > 0) {
            message = MessageFormatter.arrayFormat(format, params).getMessage();
        }

        LiveListenerBus listenerBus = SparkSession.active().sparkContext().listenerBus();
        JobServerLogEvent logEvent = new JobServerLogEvent(LogLevel.WARN, message);
        listenerBus.post(logEvent);
        LOG.info(format, params);
    }

    public static void error(String format, Object... params) {
        String message = format;
        if (params.length > 0) {
            message = MessageFormatter.arrayFormat(format, params).getMessage();
        }

        LiveListenerBus listenerBus = SparkSession.active().sparkContext().listenerBus();
        JobServerLogEvent logEvent = new JobServerLogEvent(LogLevel.ERROR, message);
        listenerBus.post(logEvent);
        LOG.info(format, params);
    }

    public static void stdout(String format, Object... params) {
        String message = format;
        if (params.length > 0) {
            message = MessageFormatter.arrayFormat(format, params).getMessage();
        }

        LiveListenerBus listenerBus = SparkSession.active().sparkContext().listenerBus();
        JobServerLogEvent logEvent = new JobServerLogEvent(LogLevel.STDOUT, message);
        listenerBus.post(logEvent);
    }
}
