package com.dataworks.datatunnel.clickhouse;

import com.dataworks.datatunnel.api.DataTunnelException;
import com.dataworks.datatunnel.api.DataTunnelSink;
import com.dataworks.datatunnel.clickhouse.constant.ClickHouseWriterOption;
import com.dataworks.datatunnel.common.util.AESUtil;
import com.gitee.melin.bee.util.MapperUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static com.dataworks.datatunnel.clickhouse.constant.ClickHouseWriterOption.*;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class ClickhouseWriter implements DataTunnelSink {

    private static final Logger logger = LoggerFactory.getLogger(ClickhouseWriter.class);

    @Override
    public void validateOptions(Map<String, String> options) throws IOException {
        logger.info("ClickhouseWriter options = {}",  MapperUtils.toJSONString(options));
        String dataSourceCode = options.get(DATASOURCE_CODE);
        if (StringUtils.isBlank(dataSourceCode)){
            throw new DataTunnelException("缺少code参数");
        } else {
            String config = options.get(DATASOURCE_CONFIG);
            logger.info("ClickhouseWriter config = {}",  config);
            if (StringUtils.isBlank(config)){
                throw new DataTunnelException("数据源未配置");
            }
        }
        if (StringUtils.isBlank(options.get(TABLE_NAME))){
            throw new DataTunnelException("缺少table参数");
        }
        if (Objects.nonNull(options.get(ClickHouseWriterOption.NUM_PARTITIONS))){
            if (NumberUtils.toInt(options.get(ClickHouseWriterOption.NUM_PARTITIONS)) <= 0){
                throw new DataTunnelException("numPartitions参数设置错误");
            }
        }
    }

    @Override
    public void write(SparkSession sparkSession, Dataset<Row> dataset, Map<String, String> options) throws IOException {
        //实例编号
        String jobInstanceCode = sparkSession.sparkContext().getConf().get("spark.datawork.job.code");
        if (StringUtils.isBlank(jobInstanceCode)){
            throw new DataTunnelException("实例编号为空");
        }

        if (0 == dataset.count()){
            throw new DataTunnelException("dataset为空");
        }
        String tableName = options.get(TABLE_NAME);
        String datasourceConfig = options.get(DATASOURCE_CONFIG);
        Map<String, Object> datasourceMap = MapperUtils.toJavaMap(datasourceConfig);
        dataset.write()
                .mode(SaveMode.Append)
                .jdbc(getCKJdbcUrl(options, datasourceMap), tableName, getCKJdbcProperties(options, datasourceMap));
    }

    private String getCKJdbcUrl(Map<String, String> options, Map<String, Object> datasourceMap){
        String databaseName = options.getOrDefault(DATABASE_NAME, (String) datasourceMap.get(SCHEMA));
        return String.format("jdbc:clickhouse://%s:%s/%s", datasourceMap.get(HOST), datasourceMap.get(PORT), databaseName);
    }

    private Properties getCKJdbcProperties(Map<String, String> options, Map datasourceMap){
        Properties properties = new Properties();
        properties.put("driver", "cc.blynk.clickhouse.ClickHouseDriver");
        properties.put("user", datasourceMap.get(USERNAME));
        properties.put("password", AESUtil.decrypt((String) datasourceMap.get(PASSWORD)));
        properties.put("batchsize", options.getOrDefault(BATCH_SIZE, "200000"));
        properties.put("socket_timeout", "300000");
        properties.put("numPartitions", options.getOrDefault(NUM_PARTITIONS, "8"));
        properties.put("rewriteBatchedStatements", options.getOrDefault(REWRITE_BATCHED_STATEMENTS, "true"));
        return properties;
    }
}
