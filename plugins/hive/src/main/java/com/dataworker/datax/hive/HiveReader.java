package com.dataworker.datax.hive;

import com.dataworker.datax.api.DataXException;
import com.dataworker.datax.api.DataxReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class HiveReader implements DataxReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveReader.class);

    @Override
    public void validateOptions(Map<String, String> options) {
        String tableName = options.get("tableName");
        if (StringUtils.isBlank(tableName)) {
            throw new DataXException("tableName 不能为空");
        }

        String columns = options.get("columns");
        if (StringUtils.isBlank(columns)) {
            throw new DataXException("columns 不能为空");
        }
    }

    @Override
    public Dataset<Row> read(SparkSession sparkSession, Map<String, String> options) throws IOException {
        String tableName = options.get("tableName");
        String partitions = options.get("partition");
        String columns = options.get("column");
        String condition = options.get("condition");

        boolean isPart = checkPartition(sparkSession, tableName, partitions);
        StringBuilder sqlBuilder = new StringBuilder("select ");
        sqlBuilder.append(columns).append(" from ");

        if (isPart) {
            sqlBuilder.append("where ");
            partitions = StringUtils.replace(partitions, "/", " and ");
            partitions = StringUtils.replace(partitions, ",", " or ");
        }

        sqlBuilder.append("(").append(partitions).append(") ");

        if (StringUtils.isNoneBlank(condition)) {
            if (isPart) {
                sqlBuilder.append("and ").append(condition);
            } else {
                sqlBuilder.append("where ").append(condition);
            }
        }

        String sql = sqlBuilder.toString();
        LOGGER.info("exec sql: {}", sql);

        return sparkSession.sql(sql);
    }

    /**
     * 校验分区表必须指定分区, 如果是分区表返回 true
     * @param sparkSession
     * @param tableName
     * @param partitions
     */
    private boolean checkPartition(SparkSession sparkSession, String tableName, String partitions) {
        String[] items = StringUtils.split(tableName, ".");

        try {
            if (items.length == 1) {
                TableIdentifier tableIdentifier = new TableIdentifier(tableName);
                CatalogTable table = sparkSession.sessionState().catalog().getTableMetadata(tableIdentifier);
                if (table.partitionColumnNames().size() > 0 && StringUtils.isBlank(partitions)) {
                    throw new DataXException("分区表，partitions 不能为空");
                }
            } else {
                String currentDb = sparkSession.sessionState().catalog().currentDb();
                try {
                    sparkSession.sessionState().catalog().setCurrentDatabase(items[0]);
                    TableIdentifier tableIdentifier = new TableIdentifier(items[1]);
                    CatalogTable table = sparkSession.sessionState().catalog().getTableMetadata(tableIdentifier);
                    if (table.partitionColumnNames().size() > 0 && StringUtils.isBlank(partitions)) {
                        throw new DataXException("分区表，partitions 不能为空");
                    }

                    if (table.partitionColumnNames().size() > 0) {
                        return true;
                    }
                } finally {
                    sparkSession.sessionState().catalog().setCurrentDatabase(currentDb);
                }
            }

            return false;
        } catch (NoSuchTableException e) {
            throw new DataXException(e.message());
        } catch (NoSuchDatabaseException e) {
            throw new DataXException(e.message());
        }
    }
}
