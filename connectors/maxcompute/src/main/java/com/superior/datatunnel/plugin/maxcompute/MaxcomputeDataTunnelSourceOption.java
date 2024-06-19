package com.superior.datatunnel.plugin.maxcompute;

import com.superior.datatunnel.api.model.BaseSourceOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import javax.validation.constraints.NotBlank;
import lombok.Data;

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

    @OptionDesc("maxcompute 不支持动态分区，如果要写入分区表，需要指定分区值，" + "例如：pt='20230605'; 或者读取多个分区: pt>'20230718' and pt<='20230719'")
    private String partitionSpec;

    private String condition;
}
