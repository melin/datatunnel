package com.dataworks.datatunnel.hive;

import com.dataworks.datatunnel.api.DataTunnelException;
import com.dataworks.datatunnel.api.DataTunnelSource;
import com.dataworks.datatunnel.common.util.CommonUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class HiveReader implements DataTunnelSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveReader.class);

    @Override
    public void validateOptions(Map<String, String> options) {
        String tableName = options.get("tableName");
        if (StringUtils.isBlank(tableName)) {
            throw new DataTunnelException("tableName 不能为空");
        }

        String column = options.get("column");
        if (StringUtils.isBlank(column)) {
            throw new DataTunnelException("column 不能为空");
        }
    }

    @Override
    public Dataset<Row> read(SparkSession sparkSession, Map<String, String> options) throws IOException {
        String databaseName = options.get("databaseName");
        String tableName = options.get("tableName");
        String column = options.get("column");
        List<String> columns = CommonUtils.parseColumn(column);
        String condition = options.get("condition");

        String table = tableName;
        if (StringUtils.isNotBlank(databaseName)) {
            table = databaseName + "." + tableName;
        }

        StringBuilder sqlBuilder = new StringBuilder("select ");
        sqlBuilder.append(StringUtils.join(columns, ",")).append(" from ").append(table);

        if (StringUtils.isNotBlank(condition)) {
            sqlBuilder.append(" where ").append(condition);
        }

        String sql = sqlBuilder.toString();
        LOGGER.info("exec sql: {}", sql);

        return sparkSession.sql(sql);
    }
}
