package com.superior.datatunnel.plugin.doris;

import com.superior.datatunnel.api.ParamKey;
import com.superior.datatunnel.api.model.BaseSinkOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import lombok.Data;

import javax.validation.constraints.NotBlank;

@Data
public class DorisDataTunnelSinkOption extends BaseSinkOption {

    @ParamKey("doris.fenodes")
    @OptionDesc("Doris FE http 地址，支持多个地址，使用逗号分隔")
    @NotBlank
    private String fenodes;

    @OptionDesc("访问Doris的用户名")
    @NotBlank
    private String user;

    @OptionDesc("访问Doris的用户名")
    @NotBlank
    private String password;

    @ParamKey("doris.table.identifier")
    @OptionDesc("Doris 表名，如：db1.tbl1")
    @NotBlank
    private String tableName;

    @ParamKey("sink.batch.size")
    @OptionDesc("单次写BE的最大行数")
    private int batchSize = 10000;

    @ParamKey("sink.max-retries")
    @OptionDesc("写BE失败之后的重试次数")
    private int sinkRetries = 1;

    @ParamKey("doris.sink.task.partition.size")
    @OptionDesc("Doris写入任务对应的 Partition 个数。Spark RDD 经过过滤等操作，最后写入的 Partition 数可能会比较大，但每个 Partition 对应的记录数比较少，导致写入频率增加和计算资源浪费。\n" +
            "此数值设置越小，可以降低 Doris 写入频率，减少 Doris 合并压力。该参数配合 doris.sink.task.use.repartition 使用。")
    private Integer partitionSize;

    @ParamKey("doris.sink.task.use.repartition\t")
    @OptionDesc("是否采用 repartition 方式控制 Doris写入 Partition数。默认值为 false，采用 coalesce 方式控制（注意: 如果在写入之前没有 Spark action 算子，可能会导致整个计算并行度降低）。\n" +
            "如果设置为 true，则采用 repartition 方式（注意: 可设置最后 Partition 数，但会额外增加 shuffle 开销）。")
    private boolean repartition = false;

    @ParamKey("doris.sink.batch.interval.ms")
    @OptionDesc("每个批次sink的间隔时间，单位: ms。")
    private int intervalTimes = 50;

    @ParamKey("doris.ignore-type")
    @OptionDesc("指在定临时视图中，读取 schema 时要忽略的字段类型。例如，'doris.ignore-type'='bitmap,hll'")
    private String ignoreType;
}
