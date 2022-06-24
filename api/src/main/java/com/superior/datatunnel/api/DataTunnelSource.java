package com.superior.datatunnel.api;

import com.gitee.melin.bee.core.extension.SPI;
import com.superior.datatunnel.api.model.SourceOption;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author melin 2021/7/27 10:47 上午
 */
@SPI
public interface DataTunnelSource<T extends SourceOption> extends Serializable {

    Dataset<Row> read(DataTunnelSourceContext<T> context) throws IOException;
}
