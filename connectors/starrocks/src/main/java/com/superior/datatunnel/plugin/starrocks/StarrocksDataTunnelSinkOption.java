package com.superior.datatunnel.plugin.starrocks;

import com.superior.datatunnel.api.ParamKey;
import com.superior.datatunnel.api.model.BaseSinkOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import com.superior.datatunnel.common.enums.WriteMode;
import lombok.Data;

import javax.validation.constraints.NotBlank;

@Data
public class StarrocksDataTunnelSinkOption extends BaseSinkOption {

    @ParamKey("table.identifier")
    @OptionDesc("StarRocks 目标表的名称，格式为 <database_name>.<table_name>")
    @NotBlank
    private String tableName;

    @ParamKey("user")
    @OptionDesc("StarRocks 集群账号的用户名")
    @NotBlank
    private String user;

    @ParamKey("password")
    @OptionDesc("StarRocks 集群账号的密码")
    @NotBlank
    private String password;

    @ParamKey("fe.http.url")
    @OptionDesc("FE 的 HTTP 地址，支持输入多个FE地址，使用逗号分隔")
    @NotBlank
    private String feHttpUrl;

    @ParamKey("fe.jdbc.url")
    @OptionDesc("FE 的 MySQL Server 连接地址")
    @NotBlank
    private String feJdbcUrl;

    @ParamKey("write.label.prefix")
    @OptionDesc("指定Stream Load使用的label的前缀")
    private String writeLabelPrefix = "spark-";

    @ParamKey("write.enable.transaction-stream-load")
    @OptionDesc("是否使用Stream Load的事务接口导入数据，详见Stream Load事务接口，该功能需要StarRocks 2.4及以上版本")
    private Boolean transactionEnabled = true;

    @ParamKey("write.buffer.size")
    @OptionDesc("是否使用Stream Load的事务接口导入数据，详见Stream Load事务接口，该功能需要StarRocks 2.4及以上版本")
    private Long writeBufferSize = 104857600L;

    @ParamKey("write.flush.interval.ms")
    @OptionDesc("数据攒批发送的间隔，用于控制数据写入StarRocks的延迟。")
    private Long writeFlushInterval = 300000L;

    @ParamKey("columns")
    @OptionDesc("支持向 StarRocks 表中写入部分列，通过该参数指定列名，多个列名之间使用逗号 (,) 分隔，例如\"c0,c1,c2\"")
    private String columns;

    @ParamKey("write.num.partitions")
    @OptionDesc("Spark用于并行写入的分区数，数据量小时可以通过减少分区数降低导入并发和频率，默认分区数由Spark决定。使用该功能可能会引入 Spark Shuffle cost")
    private Integer writePartitionNum;

    @ParamKey("write.partition.columns")
    @OptionDesc("用于Spark分区的列，只有指定 starrocks.write.num.partitions 后才有效，如果不指定则使用所有写入的列进行分区")
    private String writePartitionColumns;
}
