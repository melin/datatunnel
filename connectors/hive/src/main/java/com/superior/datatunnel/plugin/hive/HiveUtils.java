package com.superior.datatunnel.plugin.hive;

import com.gitee.melin.bee.util.JsonUtils;
import com.superior.datatunnel.api.DataTunnelException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.Map;

/**
 * @author melin 2021/11/9 3:11 下午
 */
public class HiveUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveUtils.class);

    public static boolean checkPartition(SparkSession sparkSession, String databaseName, String tableName) {
        try {
            if (StringUtils.isBlank(databaseName)) {
                TableIdentifier tableIdentifier = new TableIdentifier(tableName);
                CatalogTable table = sparkSession.sessionState().catalog().getTableMetadata(tableIdentifier);

                String[] partitionNames = queryPartitionsColumns(table);
                LOGGER.info("tableId: {}, partitionNames: {}", table, StringUtils.join(partitionNames, ","));
                if (partitionNames != null && partitionNames.length > 0) {
                    return true;
                }
            } else {
                String currentDb = sparkSession.sessionState().catalog().currentDb();
                try {
                    TableIdentifier tableIdentifier = new TableIdentifier(tableName, Option.apply(databaseName));
                    CatalogTable table = sparkSession.sessionState().catalog().getTableMetadata(tableIdentifier);

                    String[] partitionNames = queryPartitionsColumns(table);
                    LOGGER.info("tableId: {}, partitionNames: {}", table, StringUtils.join(partitionNames, ","));
                    if (partitionNames != null && partitionNames.length > 0) {
                        return true;
                    }
                } finally {
                    sparkSession.sessionState().catalog().setCurrentDatabase(currentDb);
                }
            }
        } catch (Exception e) {
            throw new DataTunnelException("检测表是否分区失败: " + e.getMessage(), e);
        }

        return false;
    }

    private static String[] queryPartitionsColumns(CatalogTable table) {
        Map<String, String> properties = table.properties();
        if (table.partitionColumnNames().size() > 0) {
            return table.partitionColumnNames().mkString(",").split(",");
        } else if (properties.contains("table_type")) {
            String tableType = properties.get("table_type").get();
            if (StringUtils.equals(tableType, "PAIMON")) {
                if (properties.contains("partition")) {
                    String partition = properties.get("partition").get();
                    return StringUtils.split(partition, ",");
                }
            } else if (StringUtils.equals(tableType, "ICEBERG")) {
                if (properties.contains("default-partition-spec")) {
                    String json = properties.get("default-partition-spec").get();
                    LinkedHashMap<String, Object> partitionSpec = JsonUtils.toJavaMap(json);

                    ArrayList<LinkedHashMap<String, Object>> fields =
                            (ArrayList<LinkedHashMap<String, Object>>) partitionSpec.get("fields");

                    return fields.stream().map(map -> (String) map.get("name")).toArray(String[]::new);
                }
            }
        }

        return null;
    }
}
