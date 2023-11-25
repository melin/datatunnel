package com.superior.datatunnel.plugin.maxcompute;

import com.superior.datatunnel.api.model.BaseSourceOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import lombok.Data;

import javax.validation.constraints.NotBlank;

@Data
public class MaxcomputeDataTunnelSourceOption extends BaseSourceOption {

    private String projectName;

    @OptionDesc("等同 projectName, projectName 和 schemaName 只需设置一个")
    private String schemaName;

    @NotBlank(message = "tableName can not blank")
    private String tableName;

    @NotBlank(message = "accessKeyId can not blank")
    private String accessKeyId;

    @NotBlank(message = "secretAccessKey can not blank")
    private String secretAccessKey;

    @NotBlank(message = "endpoint can not blank")
    private String endpoint = "http://service.cn.maxcompute.aliyun.com/api";

    private String condition;
}
