package com.superior.datatunnel.plugin.hdfs;

import lombok.Data;

import javax.validation.constraints.NotNull;

@Data
public class HdfsDataTunnelSourceOption extends HdfsCommonOption {

    @NotNull(message = "paths can not null, 支持多个参数")
    private String[] paths;
}
