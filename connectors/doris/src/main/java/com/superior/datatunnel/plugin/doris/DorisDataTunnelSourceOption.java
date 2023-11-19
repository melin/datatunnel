package com.superior.datatunnel.plugin.doris;

import com.superior.datatunnel.api.ParamKey;
import com.superior.datatunnel.api.model.BaseSourceOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import lombok.Data;

import javax.validation.constraints.NotBlank;

@Data
public class DorisDataTunnelSourceOption extends BaseSourceOption {

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

    @ParamKey("doris.request.retries")
    @OptionDesc("向Doris发送请求的重试次数")
    private int retries;

    @ParamKey("doris.request.connect.timeout.ms")
    @OptionDesc("向Doris发送请求的连接超时时间, 单位: ms")
    private int connectTimeout = 30000;

    @ParamKey("doris.request.read.timeout.ms")
    @OptionDesc("向Doris发送请求的读取超时时间, 单位: ms")
    private int readTimeout = 30000;

    @ParamKey("doris.request.query.timeout.s")
    @OptionDesc("查询doris的超时时间，默认值为1小时，-1表示无超时限制, 单位: s")
    private int queryTimeout = 3600;

    @ParamKey("doris.request.tablet.size")
    @OptionDesc("一个RDD Partition对应的Doris Tablet个数。此数值设置越小，则会生成越多的Partition。从而提升Spark侧的并行度，但同时会对Doris造成更大的压力。")
    private int tabletSize = Integer.MAX_VALUE;

    @ParamKey("doris.batch.size")
    @OptionDesc("一次从BE读取数据的最大行数。增大此数值可减少Spark与Doris之间建立连接的次数。从而减轻网络延迟所带来的额外时间开销。")
    private int batchSize = 1024;

    @ParamKey("doris.exec.mem.limit")
    @OptionDesc("单个查询的内存限制。默认为 2GB，单位为字节")
    private long memLimit = 2147483648L;

    @ParamKey("doris.deserialize.arrow.async")
    @OptionDesc("是否支持异步转换Arrow格式到spark-doris-connector迭代所需的RowBatch")
    private boolean deserializeArrowAsync = false;

    @ParamKey("doris.deserialize.queue.size")
    @OptionDesc("异步转换Arrow格式的内部处理队列，当doris.deserialize.arrow.async为true时生效")
    private int deserializeQueueSize = 64;

    @ParamKey("doris.filter.query.in.max.count")
    @OptionDesc("谓词下推中，in表达式value列表元素最大数量。超过此数量，则in表达式条件过滤在Spark侧处理。")
    private int filterQueryInMaxCount = 100;

    @ParamKey("doris.ignore-type")
    @OptionDesc("指在定临时视图中，读取 schema 时要忽略的字段类型。例如，'doris.ignore-type'='bitmap,hll'")
    private String ignoreType;
}
