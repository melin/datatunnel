package com.superior.datatunnel.elasticsearch;

import com.superior.datatunnel.api.model.SinkOption;
import lombok.Data;

import javax.validation.constraints.NotBlank;

@Data
public class EsSinkOption extends SinkOption {

    @NotBlank(message = "hosts can not blank")
    private String hosts;

    @NotBlank(message = "resource can not blank")
    private String resource;
}
