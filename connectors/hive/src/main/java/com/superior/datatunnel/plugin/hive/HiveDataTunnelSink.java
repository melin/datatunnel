package com.superior.datatunnel.plugin.hive;

import com.clearspring.analytics.util.Lists;
import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.common.enums.FileFormat;
import com.superior.datatunnel.common.enums.WriteMode;
import com.superior.datatunnel.common.util.HttpClientUtils;
import io.github.melin.jobserver.spark.api.LogUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.util.CharVarcharUtils;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.superior.datatunnel.common.enums.WriteMode.APPEND;
import static com.superior.datatunnel.common.enums.WriteMode.OVERWRITE;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class HiveDataTunnelSink implements DataTunnelSink {

    private static final Logger LOG = LoggerFactory.getLogger(HiveDataTunnelSink.class);

    private static final FileFormat[] SUPPORT_FORMAT =
            new FileFormat[] {FileFormat.ORC, FileFormat.PARQUET,
                    FileFormat.HUDI, FileFormat.ICEBERG};

    private void validate(HiveDataTunnelSinkOption sinkOption) {
        if (!ArrayUtils.contains(SUPPORT_FORMAT, sinkOption.getFileFormat())) {
            throw new DataTunnelException("FileFormat 仅支持：orc、parquet、hudi、iceberg");
        }
    }

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        HiveDataTunnelSinkOption sinkOption = (HiveDataTunnelSinkOption) context.getSinkOption();
        validate(sinkOption);

        try {
            String databaseName = sinkOption.getDatabaseName();
            if (StringUtils.isBlank(databaseName)) {
                databaseName = sinkOption.getSchemaName();
            }

            if (StringUtils.isBlank(databaseName)) {
                throw new IllegalArgumentException("databaseName can not blank");
            }

            String tableName = sinkOption.getTableName();
            String partitionSpec = sinkOption.getPartitionSpec();
            WriteMode writeMode = sinkOption.getWriteMode();

            boolean isPartition = HiveUtils.checkPartition(context.getSparkSession(), databaseName, tableName);
            if (isPartition && StringUtils.isBlank(partitionSpec)) {
                throw new DataTunnelException("写入表为分区表，请指定写入分区");
            } else {
                // 如果sink partitionSpec 指定了静态分区值，需要删除source dataframe 中同名列
                String[] fieldNames = dataset.schema().fieldNames();
                for (String part : partitionSpec.split(",")) {
                    String[] items = StringUtils.split(part, "=");
                    if (items.length == 2 && ArrayUtils.contains(fieldNames, items[0])) {
                        dataset = dataset.drop(items[0]);
                        LOG.info("delete column: {}", items[0]);
                    }
                }
            }

            String table = tableName;
            if (StringUtils.isNotBlank(databaseName)) {
                table = databaseName + "." + tableName;
            }

            // 控制文件数量
            String querySql = "";
            String tdlName = "tdl_datatunnel_" + System.currentTimeMillis();
            dataset.createTempView(tdlName);
            if (sinkOption.getRebalance() != null && sinkOption.getRebalance() > 1) {
                querySql = "select /*+ REBALANCE(" + sinkOption.getRebalance() + ") */ * from " + tdlName;
            } else {
                querySql = "select * from " + tdlName;
            }

            String sql = "";
            if (APPEND == writeMode) {
                if (isPartition) {
                    sql = "insert into table " + table + " partition(" + partitionSpec + ") " + querySql;
                } else {
                    sql = "insert into table " + table + " " + querySql;
                }
            } else if (OVERWRITE == writeMode) {
                if (isPartition) {
                    sql = "insert overwrite table " + table + " partition(" + partitionSpec + ") " + querySql;
                } else {
                    sql = "insert overwrite table " + table + " " + querySql;
                }
            } else {
                throw new DataTunnelException("不支持的写入模式：" + writeMode);
            }

            if (FileFormat.ORC == sinkOption.getFileFormat()) {
                context.getSparkSession().sql("set spark.sql.orc.compression.codec=" + sinkOption.getCompression());
            } else if (FileFormat.PARQUET == sinkOption.getFileFormat()
                    || FileFormat.HUDI == sinkOption.getFileFormat()) { //hudi 默认parquet 格式
                context.getSparkSession().sql("set spark.sql.parquet.compression.codec=" + sinkOption.getCompression());
            }

            // 避免清空表所有分区
            context.getSparkSession().sql("set spark.sql.sources.partitionOverwriteMode = dynamic");
            context.getSparkSession().sql(sql);
        } catch (Exception e) {
            throw new DataTunnelException(e.getMessage(), e);
        }
    }

    @Override
    public void createTable(Dataset<Row> dataset, DataTunnelContext context) {
        HiveDataTunnelSinkOption sinkOption = (HiveDataTunnelSinkOption) context.getSinkOption();
        boolean tableExists = context.getSparkSession().catalog()
                .tableExists(sinkOption.getDatabaseName(), sinkOption.getTableName());
        if (tableExists) {
            return;
        }

        if (FileFormat.HUDI == sinkOption.getFileFormat()) {
            if (StringUtils.isBlank(sinkOption.getPrimaryKey())
                    || StringUtils.isBlank(sinkOption.getPreCombineField())) {
                throw new DataTunnelException("自动创建 hudi 表需要 primaryKey 和 preCombineField");
            }
        }

        StructType structType = dataset.schema();
        String colums = Arrays.stream(structType.fields()).map(field -> {
            String typeString = CharVarcharUtils.getRawTypeString(field.metadata())
                    .getOrElse(() -> field.dataType().catalogString());

            return field.name() + " " + typeString + " " + field.getComment().getOrElse(() -> "");
        }).collect(Collectors.joining(",\n"));

        String partitionSpec = sinkOption.getPartitionSpec();
        List<String> partColumnNames = Lists.newArrayList();
        String[] fieldNames = structType.fieldNames();
        if (StringUtils.isNotBlank(partitionSpec)) {
            // 从 ds=20231102, type='Login' 格式中，解析出分区字段。
            String[] parts = StringUtils.split(partitionSpec, ",");
            for (String partCol : parts) {
                String colName = StringUtils.split(partCol, "=")[0];
                //如果映射字段包含 分区字段，需要排除
                if (!ArrayUtils.contains(fieldNames, colName)) {
                    colums += (",\n" + colName + " string");
                }
                partColumnNames.add(colName);
            }
        }

        String sql = "create table " + sinkOption.getFullTableName() + "(\n";
        sql += colums;
        sql += "\n)\n";
        sql += "USING " + sinkOption.getFileFormat().name().toLowerCase() + "\n";
        sql += "TBLPROPERTIES (compression='" + sinkOption.getCompression().name().toLowerCase() + "'";
        if (FileFormat.HUDI == sinkOption.getFileFormat()) {
            sql += ",\n    primaryKey='" + sinkOption.getPrimaryKey() + "'";
            sql += ",\n    preCombineField='" + sinkOption.getPreCombineField() + "'";
        }
        sql += (")");

        if (!partColumnNames.isEmpty()) {
            sql += "\nPARTITIONED BY (" + StringUtils.join(partColumnNames, ",") + ")";
        }

        LOG.info("create table sql: {}", sql);
        context.getSparkSession().sql(sql);
        LogUtils.info("自动创建表: {}，同步表元数据", sinkOption.getFullTableName());
        syncTableMeta(sinkOption.getDatabaseName(), sinkOption.getTableName());
    }

    private void syncTableMeta(String databaseName, String tableName) {
        SparkSession sparkSession = SparkSession.active();
        String superiorUrl = sparkSession.conf().get("spark.jobserver.superior.url", null);
        String regionCode = sparkSession.conf().get("spark.jobserver.superior.region", null);
        String userId = sparkSession.conf().get("spark.jobserver.superior.userId", null);
        String tenantId = sparkSession.conf().get("spark.jobserver.superior.tenantId", null);
        String catalogName = System.getProperty("session.catalog.name", "spark_catalog");
        if (StringUtils.isNotBlank(superiorUrl) && userId != null) {
            superiorUrl += "/innerApi/v1/importHiveTable";
            List<NameValuePair> params = new ArrayList<>();
            params.add(new BasicNameValuePair("tenantId", tenantId));
            params.add(new BasicNameValuePair("regionCode", regionCode));
            params.add(new BasicNameValuePair("catalogName", catalogName));
            params.add(new BasicNameValuePair("databaseName", databaseName));
            params.add(new BasicNameValuePair("tableName", tableName));
            params.add(new BasicNameValuePair("userId", userId));

            HttpClientUtils.postRequet(superiorUrl, params);
        } else {
            LOG.warn("请求同步失败: superiorUrl: {}, userId: {}", superiorUrl, userId);
            LogUtils.warn("请求同步失败: superiorUrl: {}, userId: {}", superiorUrl, userId);
        }
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return HiveDataTunnelSinkOption.class;
    }
}
