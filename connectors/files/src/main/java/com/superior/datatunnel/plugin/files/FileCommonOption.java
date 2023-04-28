package com.superior.datatunnel.plugin.files;

import com.superior.datatunnel.api.model.BaseCommonOption;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import com.superior.datatunnel.common.enums.FileFormat;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Data
public class FileCommonOption extends BaseCommonOption
        implements DataTunnelSourceOption, DataTunnelSinkOption {

    @NotNull(message = "format can not null")
    private FileFormat format;

    @NotBlank(message = "filePath can not blank")
    private String filePath;

}
