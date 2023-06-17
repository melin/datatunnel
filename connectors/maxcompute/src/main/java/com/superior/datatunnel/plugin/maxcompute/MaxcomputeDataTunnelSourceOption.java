package com.superior.datatunnel.plugin.maxcompute;

import com.superior.datatunnel.api.model.BaseSourceOption;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;

@Data
public class MaxcomputeDataTunnelSourceOption extends BaseSourceOption {

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

    @NotEmpty(message = "columns can not empty")
    private String[] columns = new String[]{"*"};

    private String condition;
}
