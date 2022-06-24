package com.superior.datatunnel.api;

import com.superior.datatunnel.api.model.SinkOption;
import com.superior.datatunnel.api.model.SourceOption;
import lombok.Data;
import org.apache.spark.sql.SparkSession;

@Data
public class DataTunnelSinkContext<R extends SinkOption> {
    private SourceOption sourceOption;

    private R sinkOption;

    private SparkSession sparkSession = SparkSession.active();
}
