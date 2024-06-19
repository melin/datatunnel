package com.superior.datatunnel.plugin.hbase;

import com.superior.datatunnel.api.model.BaseSourceOption;
import javax.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class HbaseDataTunnelSourceOption extends BaseSourceOption {

    @NotBlank(message = "tableName can not blank")
    private String tableName;

    @NotBlank(message = "zookeeperQuorum can not blank")
    private String zookeeperQuorum = "localhost";
}
