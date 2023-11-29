package com.superior.datatunnel.api;

import com.gitee.melin.bee.core.extension.SPI;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

/**
 * @author melin 2021/7/27 10:47 上午
 */
@SPI
public interface DataTunnelSink extends Serializable {

    void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException;

    Class<? extends DataTunnelSinkOption> getOptionClass();

    default void createTable(Dataset<Row> dataset, DataTunnelContext context) {
    }

    default Set<String> optionalOptions() {
        return Collections.emptySet();
    }
}
