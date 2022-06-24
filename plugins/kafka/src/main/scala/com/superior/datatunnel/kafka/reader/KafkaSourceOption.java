package com.superior.datatunnel.kafka.reader;

import com.superior.datatunnel.api.ParamKey;
import com.superior.datatunnel.api.model.SourceOption;
import lombok.Data;

import javax.validation.constraints.NotBlank;

@Data
public class KafkaSourceOption extends SourceOption {

    @NotBlank(message = "subscribe can not blank")
    private String subscribe;

    @ParamKey("kafka.bootstrap.servers")
    @NotBlank(message = "kafka.bootstrap.servers can not blank")
    private String servers;
}
