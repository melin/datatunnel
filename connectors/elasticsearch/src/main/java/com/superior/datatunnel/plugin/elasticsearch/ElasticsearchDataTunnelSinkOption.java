package com.superior.datatunnel.plugin.elasticsearch;

import com.superior.datatunnel.api.model.BaseSinkOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import com.superior.datatunnel.common.enums.WriteMode;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Data
public class ElasticsearchDataTunnelSinkOption extends BaseSinkOption {

    @NotBlank(message = "nodes can not blank")
    private String nodes = "localhost";

    @NotNull(message = "port can not blank")
    private Integer port = 9200;

    @NotBlank(message = "resource can not blank")
    private String resource;

    @NotBlank(message = "indexKey can not blank")
    private String indexKey = "id";

    @OptionDesc("数据写入模式")
    @NotNull(message = "writeMode can not null")
    private WriteMode writeMode = WriteMode.INSERT;
}
