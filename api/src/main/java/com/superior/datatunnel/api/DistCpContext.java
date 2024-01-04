package com.superior.datatunnel.api;

import com.superior.datatunnel.api.model.DistCpBaseSinkOption;
import com.superior.datatunnel.api.model.DistCpBaseSourceOption;
import org.apache.spark.sql.SparkSession;

public class DistCpContext {

    private DistCpBaseSourceOption sourceOption;

    private DistCpBaseSinkOption sinkOption;

    private SparkSession sparkSession = SparkSession.active();

    public DistCpBaseSourceOption getSourceOption() {
        return sourceOption;
    }

    public void setSourceOption(DistCpBaseSourceOption sourceOption) {
        this.sourceOption = sourceOption;
    }

    public DistCpBaseSinkOption getSinkOption() {
        return sinkOption;
    }

    public void setSinkOption(DistCpBaseSinkOption sinkOption) {
        this.sinkOption = sinkOption;
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public void setSparkSession(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }
}
