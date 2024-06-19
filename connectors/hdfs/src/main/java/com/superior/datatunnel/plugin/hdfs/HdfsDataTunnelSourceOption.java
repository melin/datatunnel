package com.superior.datatunnel.plugin.hdfs;

import javax.validation.constraints.NotNull;
import lombok.Data;

@Data
public class HdfsDataTunnelSourceOption extends HdfsCommonOption {

    @NotNull(message = "paths can not null, 支持多个参数")
    private String[] paths;
}
