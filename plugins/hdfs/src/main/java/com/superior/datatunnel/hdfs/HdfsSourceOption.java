package com.superior.datatunnel.hdfs;

import com.superior.datatunnel.api.model.SourceOption;
import lombok.Data;

import javax.validation.constraints.NotBlank;

@Data
public class HdfsSourceOption extends SourceOption {
    @NotBlank(message = "path can not blank")
    private String path;

    private String fileNameSuffix;
}
