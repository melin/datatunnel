package com.superior.datatunnel.plugin.file;

import lombok.Data;

import javax.validation.constraints.NotNull;

@Data
public class FileDataTunnelSourceOption extends FileCommonOption {

    @NotNull(message = "paths can not null, 支持多个参数")
    private String[] paths;
}
