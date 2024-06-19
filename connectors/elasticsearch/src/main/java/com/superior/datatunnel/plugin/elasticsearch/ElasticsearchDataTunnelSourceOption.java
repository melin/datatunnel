package com.superior.datatunnel.plugin.elasticsearch;

import com.superior.datatunnel.api.model.BaseSourceOption;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import lombok.Data;

@Data
public class ElasticsearchDataTunnelSourceOption extends BaseSourceOption {

    @NotBlank(message = "nodes can not blank")
    private String nodes = "localhost";

    @NotNull(message = "port can not blank")
    private Integer port = 9200;

    @NotBlank(message = "resource can not blank")
    private String resource;

    private String query;
}
