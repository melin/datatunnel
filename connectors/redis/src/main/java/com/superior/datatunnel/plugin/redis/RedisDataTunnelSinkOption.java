package com.superior.datatunnel.plugin.redis;

import com.superior.datatunnel.api.model.BaseSinkOption;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Data
public class RedisDataTunnelSinkOption extends BaseSinkOption {

    private String user;

    private String password;

    @NotBlank(message = "host can not blank")
    private String host;

    @NotNull(message = "port can not blank")
    private Integer port = 6379;

    private int database = 0;

    @NotNull(message = "table can not blank")
    private String table;

    @NotNull(message = "keyColumn can not blank")
    private String keyColumn;

    private int timeout = 2000; //ms

    private boolean sslEnabled = false;

    private int ttl = 0;

    private int maxPipelineSize = 100;

    private int iteratorGroupingSize = 1000;

}
