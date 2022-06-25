package com.superior.datatunnel.hdfs;

import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import lombok.Data;

import javax.validation.constraints.NotBlank;

@Data
public class HdfsDataTunnelSourceOption extends DataTunnelSourceOption {
    @NotBlank(message = "path can not blank")
    private String path;

    private String fileNameSuffix;
}
