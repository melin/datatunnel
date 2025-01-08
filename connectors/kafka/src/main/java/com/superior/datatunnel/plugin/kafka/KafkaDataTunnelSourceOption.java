package com.superior.datatunnel.plugin.kafka;

import com.superior.datatunnel.api.ParamKey;
import com.superior.datatunnel.api.model.BaseSourceOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import lombok.Data;

import javax.validation.constraints.NotBlank;

@Data
public class KafkaDataTunnelSourceOption extends BaseSourceOption {

    @NotBlank(message = "kafka value 数据格式，仅支持: json, text。")
    private String format = "text";

    private String assign;

    private String subscribe;

    private String subscribePattern;

    @ParamKey("kafka.bootstrap.servers")
    @NotBlank(message = "kafka.bootstrap.servers can not blank")
    private String servers;

    private boolean failOnDataLoss = false;

    @OptionDesc("可选值：earliest, latest")
    private String startingOffsets;

    private Long startingTimestamp;

    private String maxTriggerDelay = "15";

    private Integer minPartitions;

    private String groupIdPrefix;

    @ParamKey("kafka.group.id")
    private String kafkaGroupId;

    private boolean includeHeaders = false;

    private String startingOffsetsByTimestampStrategy = "error";

    @OptionDesc("checkpoint 存储位置")
    private String checkpointLocation;

    @OptionDesc("Trigger ProcessingTime, 单位秒，默认值：60")
    private Long triggerProcessingTime = 60L;
}
