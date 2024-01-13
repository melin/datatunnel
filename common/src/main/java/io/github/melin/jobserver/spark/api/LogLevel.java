package io.github.melin.jobserver.spark.api;

public enum LogLevel {
    STDOUT("Stdout"), INFO("Info"), WARN("Warn"), ERROR("Error"), INTANCE_INFO("IntanceInfo");

    private final String type;

    LogLevel(String type){
        this.type = type;
    }

    public String getType() {
        return type;
    }

    @Override
    public String toString() {
        return this.type;
    }
}
