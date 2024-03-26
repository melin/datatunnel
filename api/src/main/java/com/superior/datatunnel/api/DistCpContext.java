package com.superior.datatunnel.api;

import com.superior.datatunnel.api.model.DistCpOption;
import org.apache.spark.sql.SparkSession;

public class DistCpContext {

    private DistCpOption option;

    private SparkSession sparkSession = SparkSession.active();

    public DistCpOption getOption() {
        return option;
    }

    public void setOption(DistCpOption option) {
        this.option = option;
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public void setSparkSession(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }
}
