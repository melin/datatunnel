package com.superior.datatunnel.plugin.kafka;

import com.superior.datatunnel.api.ParamKey;
import com.superior.datatunnel.api.model.BaseSourceOption;
import com.superior.datatunnel.common.annotation.OptionDesc;

import javax.validation.constraints.NotBlank;

public class KafkaDataTunnelSourceOption extends BaseSourceOption {

    @NotBlank(message = "如果是json 格式，解析第一层")
    private String format = "text";

    private String assign;

    private String subscribe;

    private String subscribePattern;

    @ParamKey("kafka.bootstrap.servers")
    @NotBlank(message = "kafka.bootstrap.servers can not blank")
    private String servers;

    private boolean failOnDataLoss = false;

    @NotBlank(message = "startingOffsets")
    @NotBlank(message = "可选值：earliest, latest")
    private String startingOffsets = "latest";

    private String maxTriggerDelay = "15";

    private Integer minPartitions;

    private String groupIdPrefix;

    @ParamKey("kafka.group.id")
    private String kafkaGroupId;

    private boolean includeHeaders = false;

    private String startingOffsetsByTimestampStrategy = "error";

    @OptionDesc("checkpoint 存储位置")
    @NotBlank(message = "checkpointLocation can not blank")
    private String checkpointLocation;

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getAssign() {
        return assign;
    }

    public void setAssign(String assign) {
        this.assign = assign;
    }

    public String getSubscribe() {
        return subscribe;
    }

    public void setSubscribe(String subscribe) {
        this.subscribe = subscribe;
    }

    public String getSubscribePattern() {
        return subscribePattern;
    }

    public void setSubscribePattern(String subscribePattern) {
        this.subscribePattern = subscribePattern;
    }

    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }

    public boolean isFailOnDataLoss() {
        return failOnDataLoss;
    }

    public void setFailOnDataLoss(boolean failOnDataLoss) {
        this.failOnDataLoss = failOnDataLoss;
    }

    public String getStartingOffsets() {
        return startingOffsets;
    }

    public void setStartingOffsets(String startingOffsets) {
        this.startingOffsets = startingOffsets;
    }

    public String getMaxTriggerDelay() {
        return maxTriggerDelay;
    }

    public void setMaxTriggerDelay(String maxTriggerDelay) {
        this.maxTriggerDelay = maxTriggerDelay;
    }

    public Integer getMinPartitions() {
        return minPartitions;
    }

    public void setMinPartitions(Integer minPartitions) {
        this.minPartitions = minPartitions;
    }

    public String getGroupIdPrefix() {
        return groupIdPrefix;
    }

    public void setGroupIdPrefix(String groupIdPrefix) {
        this.groupIdPrefix = groupIdPrefix;
    }

    public String getKafkaGroupId() {
        return kafkaGroupId;
    }

    public void setKafkaGroupId(String kafkaGroupId) {
        this.kafkaGroupId = kafkaGroupId;
    }

    public boolean isIncludeHeaders() {
        return includeHeaders;
    }

    public void setIncludeHeaders(boolean includeHeaders) {
        this.includeHeaders = includeHeaders;
    }

    public String getStartingOffsetsByTimestampStrategy() {
        return startingOffsetsByTimestampStrategy;
    }

    public void setStartingOffsetsByTimestampStrategy(String startingOffsetsByTimestampStrategy) {
        this.startingOffsetsByTimestampStrategy = startingOffsetsByTimestampStrategy;
    }

    public String getCheckpointLocation() {
        return checkpointLocation;
    }

    public void setCheckpointLocation(String checkpointLocation) {
        this.checkpointLocation = checkpointLocation;
    }

}
