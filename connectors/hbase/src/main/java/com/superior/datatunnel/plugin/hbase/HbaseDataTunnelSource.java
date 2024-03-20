package com.superior.datatunnel.plugin.hbase;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.spark.HBaseContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class HbaseDataTunnelSource implements DataTunnelSource {

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        SparkSession sparkSession = context.getSparkSession();
        HbaseDataTunnelSourceOption option = (HbaseDataTunnelSourceOption) context.getSourceOption();

        HBaseConfiguration conf = new HBaseConfiguration();
        option.getProperties().forEach(conf::set);
        conf.set("hbase.zookeeper.quorum", option.getZookeeperQuorum());
        // the latest HBaseContext will be used afterwards
        new HBaseContext(sparkSession.sparkContext(), conf, null);

        DataFrameReader reader = sparkSession.read().format("org.apache.hadoop.hbase.spark")
                .option("hbase.table", option.getTableName())
                .option("hbase.columns.mapping", StringUtils.join(option.getColumns(), ","));
        option.getProperties().forEach(reader::option);

        return reader.load();
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return HbaseDataTunnelSourceOption.class;
    }
}
