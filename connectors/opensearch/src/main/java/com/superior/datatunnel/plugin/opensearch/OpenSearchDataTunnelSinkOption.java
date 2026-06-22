package com.superior.datatunnel.plugin.opensearch;

import com.superior.datatunnel.api.model.BaseSinkOption;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import lombok.Data;

@Data
public class OpenSearchDataTunnelSinkOption extends BaseSinkOption {

    @NotBlank(message = "nodes can not blank")
    private String nodes = "localhost";

    @NotNull(message = "port can not blank")
    private Integer port = 9200;

    @NotBlank(message = "index can not blank")
    private String index;

    private String indexId;
}
