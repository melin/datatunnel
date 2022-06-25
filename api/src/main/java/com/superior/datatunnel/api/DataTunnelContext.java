package com.superior.datatunnel.api;

import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import lombok.Data;
import lombok.Getter;
import org.apache.spark.sql.SparkSession;

@Data
public class DataTunnelContext {

    private DataTunnelSourceOption sourceOption;

    private DataTunnelSinkOption sinkOption;

    @Getter
    private SparkSession sparkSession = SparkSession.active();
}
