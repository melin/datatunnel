package com.superior.datatunnel.plugin.maxcompute;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author melin 2021/7/27 11:06 上午O
 */
public class MaxcomputeDataTunnelSource implements DataTunnelSource {

    private static final Logger LOG = LoggerFactory.getLogger(MaxcomputeDataTunnelSource.class);

    private static final String ODPS_DATA_SOURCE = "org.apache.spark.sql.odps.datasource";

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        MaxcomputeDataTunnelSourceOption sourceOption = (MaxcomputeDataTunnelSourceOption) context.getSourceOption();

        String projectName = sourceOption.getProjectName();
        if (StringUtils.isBlank(projectName)) {
            projectName = sourceOption.getSchemaName();
        }
        if (StringUtils.isBlank(projectName)) {
            throw new IllegalArgumentException("projectName can not blank");
        }

        Dataset<Row> dataset = context.getSparkSession()
                .read()
                .format(ODPS_DATA_SOURCE)
                .option("spark.hadoop.odps.access.id", sourceOption.getAccessKeyId())
                .option("spark.hadoop.odps.access.key", sourceOption.getSecretAccessKey())
                .option("spark.hadoop.odps.end.point", sourceOption.getEndpoint())
                .option("spark.hadoop.odps.project.name", projectName)
                .option("spark.hadoop.odps.table.name", sourceOption.getTableName())
                .load();

        try {
            String tdlName = "tdl_datatunnel_" + System.currentTimeMillis();
            dataset.createTempView(tdlName);
            String[] columns = sourceOption.getColumns();

            StringBuilder sqlBuilder = new StringBuilder("select ");
            sqlBuilder.append(StringUtils.join(columns, ",")).append(" from ").append(tdlName);

            String partitionSpec = sourceOption.getPartitionSpec();
            if (StringUtils.isNotBlank(partitionSpec)) {
                sqlBuilder.append(" where ").append(partitionSpec);
            }

            String condition = sourceOption.getCondition();
            if (StringUtils.isNotBlank(condition)) {
                if (StringUtils.isNotBlank(partitionSpec)) {
                    sqlBuilder.append(" and ").append(condition);
                } else {
                    sqlBuilder.append(" where ").append(condition);
                }
            }

            String sql = sqlBuilder.toString();
            LOG.info("exec sql: {}", sql);
            return context.getSparkSession().sql(sql);
        } catch (AnalysisException e) {
            throw new DataTunnelException(e.message(), e);
        }
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return MaxcomputeDataTunnelSourceOption.class;
    }
}
