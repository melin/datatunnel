package com.superior.datatunnel.plugin.hdfs;

import com.superior.datatunnel.api.model.BaseSourceOption;
import lombok.Data;

import javax.validation.constraints.NotBlank;

@Data
public class HdfsDataTunnelSourceOption extends BaseSourceOption {
    @NotBlank(message = "path can not blank")
    private String path;

    private String fileNameSuffix;
}
