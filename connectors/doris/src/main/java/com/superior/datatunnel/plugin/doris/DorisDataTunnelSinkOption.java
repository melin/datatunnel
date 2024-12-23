package com.superior.datatunnel.plugin.doris;

import com.superior.datatunnel.api.model.BaseSinkOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import com.superior.datatunnel.common.enums.FileFormat;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import lombok.Data;

@Data
public class DorisDataTunnelSinkOption extends BaseSinkOption {

    @OptionDesc("数据库名")
    @NotBlank
    private String databaseName;

    @OptionDesc("表名")
    @NotBlank
    private String tableName;

    @OptionDesc("doris 集群账号的用户名")
    @NotBlank
    private String username;

    @OptionDesc("doris 集群账号的密码")
    @NotNull
    private String password;

    @OptionDesc("FE 的 HTTP 地址，支持输入多个FE地址，使用逗号分隔")
    @NotBlank
    private String feEnpoints;

    @OptionDesc("Straming: 设置该选项可以将 Kafka 消息中的 value 列不经过处理直接写入. doris.sink.streaming.passthrough")
    private boolean passthrough = false;

    @OptionDesc("Stream Load 的数据格式。共支持 3 种格式：csv，json，arrow")
    private FileFormat fileFormat = FileFormat.CSV;
}
