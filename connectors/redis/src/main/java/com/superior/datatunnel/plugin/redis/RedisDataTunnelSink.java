package com.superior.datatunnel.plugin.redis;

import com.superior.datatunnel.api.*;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import java.io.IOException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.redis.RedisOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class RedisDataTunnelSink implements DataTunnelSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisDataTunnelSink.class);

    private void validateOptions(DataTunnelContext context) {}

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        validateOptions(context);

        RedisDataTunnelSinkOption sinkOption = (RedisDataTunnelSinkOption) context.getSinkOption();

        String format = "org.apache.spark.sql.redis";

        String[] fieldNames = dataset.schema().fieldNames();
        if (!ArrayUtils.contains(fieldNames, sinkOption.getKeyColumn())) {
            throw new IllegalArgumentException("key column " + sinkOption.getKeyColumn() + " not exists");
        }

        if (StringUtils.isNotBlank(sinkOption.getValueColumn())
                && !ArrayUtils.contains(fieldNames, sinkOption.getValueColumn())) {
            throw new IllegalArgumentException("value column " + sinkOption.getValueColumn() + " not exists");
        }

        DataFrameWriter writer = dataset.write().format(format);
        writer.option(RedisOptions.REDIS_HOST(), sinkOption.getHost());
        writer.option(RedisOptions.REDIS_PORT(), sinkOption.getPort());
        writer.option(RedisOptions.REDIS_USER(), sinkOption.getUser());
        writer.option(RedisOptions.REDIS_PASSWORD(), sinkOption.getPassword());
        writer.option(RedisOptions.REDIS_DATABASE(), sinkOption.getDatabase());
        writer.option(RedisOptions.REDIS_TABLE(), sinkOption.getTable());
        writer.option(RedisOptions.REDIS_KEY_COLUMN(), sinkOption.getKeyColumn());
        writer.option(RedisOptions.REDIS_VALUE_COLUMN(), sinkOption.getValueColumn());
        writer.option(RedisOptions.REDIS_TTL(), sinkOption.getTtl());
        writer.option(RedisOptions.REDIS_TIMEOUT(), sinkOption.getTimeout());
        writer.option(RedisOptions.REDIS_SSL_ENABLED(), sinkOption.isSslEnabled());
        writer.option(RedisOptions.REDIS_MAX_PIPELINE_SIZE(), sinkOption.getMaxPipelineSize());
        writer.option(RedisOptions.REDIS_ITERATOR_GROUPING_SIZE(), sinkOption.getIteratorGroupingSize());
        writer.save();
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return RedisDataTunnelSinkOption.class;
    }
}
