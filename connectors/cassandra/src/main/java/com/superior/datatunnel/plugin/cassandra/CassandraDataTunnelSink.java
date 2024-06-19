package com.superior.datatunnel.plugin.cassandra;

import com.datastax.spark.connector.datasource.CassandraCatalog;
import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.common.util.CommonUtils;
import java.io.IOException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class CassandraDataTunnelSink implements DataTunnelSink {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraDataTunnelSink.class);

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        SparkSession sparkSession = context.getSparkSession();
        CassandraDataTunnelSinkOption option = (CassandraDataTunnelSinkOption) context.getSinkOption();
        sparkSession.conf().set("spark.sql.catalog.datatunnel_cassandra", CassandraCatalog.class.getName());

        CommonUtils.convertOptionToSparkConf(sparkSession, option);

        try {
            String tdlName = "tdl_datatunnel_" + System.currentTimeMillis();
            dataset.createTempView(tdlName);
            String ckTableName = "datatunnel_cassandra." + option.getKeyspace() + "." + option.getTableName();
            String sql = "insert into " + ckTableName + " select * from " + tdlName;
            sparkSession.sql(sql);
        } catch (Exception e) {
            throw new DataTunnelException(e.getMessage(), e);
        }
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return CassandraDataTunnelSinkOption.class;
    }
}
