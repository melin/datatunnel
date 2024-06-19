package com.superior.datatunnel.api;

import com.gitee.melin.bee.core.extension.SPI;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author melin 2021/7/27 10:47 上午
 */
@SPI
public interface DataTunnelSource extends Serializable {

    Dataset<Row> read(DataTunnelContext context) throws IOException, URISyntaxException;

    Class<? extends DataTunnelSourceOption> getOptionClass();

    default boolean supportCte() {
        return false;
    }

    default Set<String> optionalOptions() {
        return Collections.emptySet();
    }
}
