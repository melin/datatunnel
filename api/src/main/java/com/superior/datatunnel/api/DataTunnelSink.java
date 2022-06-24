package com.superior.datatunnel.api;

import com.gitee.melin.bee.core.extension.SPI;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author melin 2021/7/27 10:47 上午
 */
@SPI
public interface DataTunnelSink extends Serializable {

    void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException;
}
