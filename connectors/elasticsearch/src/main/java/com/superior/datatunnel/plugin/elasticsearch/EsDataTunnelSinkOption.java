package com.superior.datatunnel.plugin.elasticsearch;

import com.superior.datatunnel.api.model.BaseSinkOption;
import lombok.Data;

import javax.validation.constraints.NotBlank;

@Data
public class EsDataTunnelSinkOption extends BaseSinkOption {

    @NotBlank(message = "hosts can not blank")
    private String hosts;

    @NotBlank(message = "resource can not blank")
    private String resource;
}
