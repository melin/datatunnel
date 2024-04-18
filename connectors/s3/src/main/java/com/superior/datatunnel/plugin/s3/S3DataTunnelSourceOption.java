package com.superior.datatunnel.plugin.s3;

import lombok.Data;

import javax.validation.constraints.NotNull;

@Data
public class S3DataTunnelSourceOption extends S3CommonOption {

    @NotNull(message = "paths can not null, 支持多个参数")
    private String[] paths;
}
