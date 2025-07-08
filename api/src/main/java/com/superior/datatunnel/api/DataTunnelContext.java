package com.superior.datatunnel.api;

import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import org.apache.spark.sql.SparkSession;

public class DataTunnelContext {

    private String sql;

    private DataSourceType sourceType;

    private DataTunnelSourceOption sourceOption;

    private DataSourceType sinkType;

    private DataTunnelSinkOption sinkOption;

    private String transfromSql;

    private SparkSession sparkSession = SparkSession.active();

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public DataSourceType getSourceType() {
        return sourceType;
    }

    public void setSourceType(DataSourceType sourceType) {
        this.sourceType = sourceType;
    }

    public DataSourceType getSinkType() {
        return sinkType;
    }

    public void setSinkType(DataSourceType sinkType) {
        this.sinkType = sinkType;
    }

    public DataTunnelSourceOption getSourceOption() {
        return sourceOption;
    }

    public void setSourceOption(DataTunnelSourceOption sourceOption) {
        this.sourceOption = sourceOption;
    }

    public DataTunnelSinkOption getSinkOption() {
        return sinkOption;
    }

    public void setSinkOption(DataTunnelSinkOption sinkOption) {
        this.sinkOption = sinkOption;
    }

    public String getTransfromSql() {
        return transfromSql;
    }

    public void setTransfromSql(String transfromSql) {
        this.transfromSql = transfromSql;
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public void setSparkSession(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }
}
