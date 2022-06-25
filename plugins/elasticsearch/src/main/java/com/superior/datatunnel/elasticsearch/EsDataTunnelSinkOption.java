package com.superior.datatunnel.elasticsearch;

import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import lombok.Data;

import javax.validation.constraints.NotBlank;

@Data
public class EsDataTunnelSinkOption extends DataTunnelSinkOption {

    @NotBlank(message = "hosts can not blank")
    private String hosts;

    @NotBlank(message = "resource can not blank")
    private String resource;
}
