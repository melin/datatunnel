package com.superior.datatunnel.plugin.maxcompute;

import com.superior.datatunnel.api.model.BaseSinkOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import com.superior.datatunnel.common.enums.WriteMode;
import javax.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class MaxcomputeDataTunnelSinkOption extends BaseSinkOption {

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

    @OptionDesc("写入模式, 仅支持：append、overwrite，不支持 upsert")
    private WriteMode writeMode = WriteMode.OVERWRITE;

    @OptionDesc("maxcompute 不支持动态分区，如果要写入分区表，需要指定分区值，例如：pt='20230605'")
    private String partitionSpec;
}
