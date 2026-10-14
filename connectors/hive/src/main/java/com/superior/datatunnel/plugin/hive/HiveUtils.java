package com.superior.datatunnel.plugin.hive;

import com.superior.datatunnel.api.DataTunnelException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author melin 2021/11/9 3:11 下午
 */
public class HiveUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveUtils.class);

    public static TableCatalog getSessionCatalog(SparkSession sparkSession, String catalogName) {
        CatalogManager catalogManager = sparkSession.sessionState().catalogManager();

        return (TableCatalog) catalogManager.catalog(catalogName);
    }

    public static boolean checkPartition(
            SparkSession sparkSession, String catalogName, String databaseName, String tableName) {
        try {
            if (StringUtils.isBlank(databaseName)) {
                databaseName = sparkSession.catalog().currentDatabase();
            }

            TableCatalog catalog = getSessionCatalog(sparkSession, catalogName);
            Identifier identifier = Identifier.of(new String[] {databaseName}, tableName);
            Table table = catalog.loadTable(identifier);
            List<String> partitionNames = queryPartitionsColumns(table);

            LOGGER.info("tableId: {}, partitionNames: {}", table, StringUtils.join(partitionNames, ","));
            if (partitionNames != null && partitionNames.size() > 0) {
                return true;
            }
        } catch (Exception e) {
            throw new DataTunnelException("检测表是否分区失败: " + e.getMessage(), e);
        }

        return false;
    }

    private static List<String> queryPartitionsColumns(Table table) {
        // 3. 获取分区变换规则数组
        Transform[] transforms = table.partitioning();
        List<String> partitionColumns = new ArrayList<>();

        for (Transform transform : transforms) {
            // 基础的单字段/多字段值分区为 IdentityTransform
            if (transform instanceof IdentityTransform) {
                IdentityTransform identity = (IdentityTransform) transform;
                // 获取分区字段名引用
                NamedReference reference = identity.reference();
                // fieldNames() 返回列名路径（普通列只有一层，如 ["dt"]）
                String colName = String.join(".", reference.fieldNames());
                partitionColumns.add(colName);
            } else {
                // 如果存在其他高级分区/分桶转换，可以通过 name() 查看类型
                System.out.println("Other transform type: " + transform.name());
            }
        }
        return partitionColumns;
    }
}
