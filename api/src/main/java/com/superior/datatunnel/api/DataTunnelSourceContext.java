package com.superior.datatunnel.api;

import com.superior.datatunnel.api.model.SinkOption;
import com.superior.datatunnel.api.model.SourceOption;
import lombok.Data;
import lombok.Getter;
import org.apache.spark.sql.SparkSession;

@Data
public class DataTunnelSourceContext<T extends SourceOption> {
    private T sourceOption;

    private SinkOption sinkOption;

    @Getter
    private SparkSession sparkSession = SparkSession.active();
}
