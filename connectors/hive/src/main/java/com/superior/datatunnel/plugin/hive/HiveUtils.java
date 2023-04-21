package com.superior.datatunnel.plugin.hive;

import com.superior.datatunnel.api.DataTunnelException;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import scala.Option;

/**
 * @author melin 2021/11/9 3:11 下午
 */
public class HiveUtils {

    public static boolean checkPartition(SparkSession sparkSession, String databaseName, String tableName) {
        try {
            if (StringUtils.isBlank(databaseName)) {
                TableIdentifier tableIdentifier = new TableIdentifier(tableName);
                CatalogTable table = sparkSession.sessionState().catalog().getTableMetadata(tableIdentifier);

                if (table.partitionColumnNames().size() > 0) {
                    return true;
                }
            } else {
                String currentDb = sparkSession.sessionState().catalog().currentDb();
                try {
                    TableIdentifier tableIdentifier = new TableIdentifier(tableName, Option.apply(databaseName));
                    CatalogTable table = sparkSession.sessionState().catalog().getTableMetadata(tableIdentifier);
                    if (table.partitionColumnNames().size() > 0) {
                        return true;
                    }
                } finally {
                    sparkSession.sessionState().catalog().setCurrentDatabase(currentDb);
                }
            }
        } catch (Exception e) {
            throw new DataTunnelException("检测表是否分区失败", e);
        }

        return false;
    }
}
