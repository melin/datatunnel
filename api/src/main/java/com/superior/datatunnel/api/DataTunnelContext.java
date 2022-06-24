package com.superior.datatunnel.api;

import com.superior.datatunnel.api.model.SinkOption;
import com.superior.datatunnel.api.model.SourceOption;
import lombok.Data;
import lombok.Getter;
import org.apache.spark.sql.SparkSession;

@Data
public class DataTunnelContext {

    private SourceOption sourceOption;

    private SinkOption sinkOption;

    @Getter
    private SparkSession sparkSession = SparkSession.active();
}
