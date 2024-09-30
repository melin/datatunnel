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

    public @NotBlank(message = "topic can not blank") String getTopic() {
        return topic;
    }

    public void setTopic(@NotBlank(message = "topic can not blank") String topic) {
        this.topic = topic;
    }

    public @NotBlank(message = "kafka.bootstrap.servers can not blank") String getServers() {
        return servers;
    }

    public void setServers(@NotBlank(message = "kafka.bootstrap.servers can not blank") String servers) {
        this.servers = servers;
    }
}
