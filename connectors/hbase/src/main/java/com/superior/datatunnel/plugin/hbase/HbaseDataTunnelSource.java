package com.superior.datatunnel.plugin.hbase;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class HbaseDataTunnelSource implements DataTunnelSource {

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        SparkSession sparkSession = context.getSparkSession();
        HbaseDataTunnelSourceOption option = (HbaseDataTunnelSourceOption) context.getSourceOption();
        sparkSession.sparkContext().hadoopConfiguration().set("hbase.zookeeper.quorum", option.getZookeeperQuorum());

        DataFrameReader reader = sparkSession
                .read()
                .format("org.apache.hadoop.hbase.spark")
                .option("hbase.table", option.getTableName())
                .option("hbase.spark.use.hbasecontext", "false")
                .option("hbase.columns.mapping", StringUtils.join(option.getColumns(), ","));
        option.getProperties().forEach(reader::option);

        Dataset<Row> df = reader.load();
        try {
            df.createTempView("test");
            return sparkSession.sql("select * from test where col1 = 'value15'");
        } catch (Exception e) {
            throw new DataTunnelException("create hbase view: " + e.getMessage(), e);
        }
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return HbaseDataTunnelSourceOption.class;
    }
}
