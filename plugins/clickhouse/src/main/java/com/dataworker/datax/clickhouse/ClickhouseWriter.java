package com.dataworker.datax.clickhouse;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dataworker.datax.api.DataXException;
import com.dataworker.datax.api.DataxWriter;
import com.dataworker.datax.clickhouse.constant.ClickHouseWriterOption;
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

import static com.dataworker.datax.clickhouse.constant.ClickHouseWriterOption.*;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class ClickhouseWriter implements DataxWriter {

    private static final Logger logger = LoggerFactory.getLogger(ClickhouseWriter.class);

    @Override
    public void validateOptions(Map<String, String> options) {
        logger.info("ClickhouseWriter options = {}",  JSON.toJSONString(options));
        String dataSourceCode = options.get(DATASOURCE_CODE);
        if (StringUtils.isBlank(dataSourceCode)){
            throw new DataXException("缺少code参数");
        } else {
            String config = options.get(DATASOURCE_CONFIG);
            logger.info("ClickhouseWriter config = {}",  config);
            if (StringUtils.isBlank(config)){
                throw new DataXException("数据源未配置");
            }
        }
        if (StringUtils.isBlank(options.get(TABLE_NAME))){
            throw new DataXException("缺少table参数");
        }
        if (Objects.nonNull(options.get(ClickHouseWriterOption.NUM_PARTITIONS))){
            if (NumberUtils.toInt(options.get(ClickHouseWriterOption.NUM_PARTITIONS)) <= 0){
                throw new DataXException("numPartitions参数设置错误");
            }
        }
    }

    @Override
    public void write(SparkSession sparkSession, Dataset<Row> dataset, Map<String, String> options) throws IOException {
        //实例编号
        String jobInstanceCode = sparkSession.sparkContext().getConf().get("spark.datawork.job.code");
        if (StringUtils.isBlank(jobInstanceCode)){
            throw new DataXException("实例编号为空");
        }

        if (0 == dataset.count()){
            throw new DataXException("dataset为空");
        }
        String tableName = options.get(TABLE_NAME);
        String datasourceConfig = options.get(DATASOURCE_CONFIG);
        Map datasourceMap = JSONObject.parseObject(datasourceConfig, Map.class);
        dataset.write()
                .mode(SaveMode.Append)
                .jdbc(getCKJdbcUrl(options, datasourceMap), tableName, getCKJdbcProperties(options, datasourceMap));
    }

    private String getCKJdbcUrl(Map<String, String> options, Map datasourceMap){
        String databaseName = options.getOrDefault(DATABASE_NAME, (String) datasourceMap.get(SCHEMA));
        return String.format("jdbc:clickhouse://%s:%s/%s", datasourceMap.get(HOST), datasourceMap.get(PORT), databaseName);
    }

    private Properties getCKJdbcProperties(Map<String, String> options, Map datasourceMap){
        Properties properties = new Properties();
        properties.put("driver", "cc.blynk.clickhouse.ClickHouseDriver");
        properties.put("user", datasourceMap.get(USERNAME));
        properties.put("password", datasourceMap.get(PASSWORD));
        properties.put("batchsize", options.getOrDefault(BATCH_SIZE, "200000"));
        properties.put("socket_timeout", "300000");
        properties.put("numPartitions", options.getOrDefault(NUM_PARTITIONS, "8"));
        properties.put("rewriteBatchedStatements", options.getOrDefault(REWRITE_BATCHED_STATEMENTS, "true"));
        return properties;
    }
}
