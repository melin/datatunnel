package com.superior.datatunnel.plugin.doris;

import com.superior.datatunnel.api.ParamKey;
import com.superior.datatunnel.api.model.BaseSourceOption;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;

import javax.validation.constraints.NotBlank;

public class DorisDataTunnelSourceOption extends BaseSourceOption {

    @NotBlank(message = "subscribe can not blank")
    private String subscribe;

    @ParamKey("kafka.bootstrap.servers")
    @NotBlank(message = "kafka.bootstrap.servers can not blank")
    private String servers;

    public String getSubscribe() {
        return subscribe;
    }

    public void setSubscribe(String subscribe) {
        this.subscribe = subscribe;
    }

    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }
}
