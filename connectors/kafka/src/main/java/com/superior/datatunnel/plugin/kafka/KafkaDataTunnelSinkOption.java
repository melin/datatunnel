package com.superior.datatunnel.plugin.kafka;

import com.superior.datatunnel.api.ParamKey;
import com.superior.datatunnel.api.model.BaseSinkOption;

import javax.validation.constraints.NotBlank;

public class KafkaDataTunnelSinkOption extends BaseSinkOption {

    @NotBlank(message = "topic can not blank")
    private String topic;

    @ParamKey("kafka.bootstrap.servers")
    @NotBlank(message = "kafka.bootstrap.servers can not blank")
    private String servers;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }
}
