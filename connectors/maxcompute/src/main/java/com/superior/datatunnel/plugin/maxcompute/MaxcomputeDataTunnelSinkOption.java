package com.superior.datatunnel.plugin.maxcompute;

import com.superior.datatunnel.api.model.BaseSinkOption;
import lombok.Data;

import javax.validation.constraints.NotBlank;

@Data
public class MaxcomputeDataTunnelSinkOption extends BaseSinkOption {

    @NotBlank(message = "projectName can not blank")
    private String projectName;

    @NotBlank(message = "tableName can not blank")
    private String tableName;

    @NotBlank(message = "accessKeyId can not blank")
    private String accessKeyId;

    @NotBlank(message = "secretAccessKey can not blank")
    private String secretAccessKey;

    @NotBlank(message = "endpoint can not blank")
    private String endpoint = "http://service.cn.maxcompute.aliyun.com/api";
}
