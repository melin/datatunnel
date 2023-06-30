package com.superior.datatunnel.plugin.starrocks;

import com.superior.datatunnel.api.ParamKey;
import com.superior.datatunnel.api.model.BaseSourceOption;
import com.superior.datatunnel.common.annotation.OptionDesc;

import javax.validation.constraints.NotBlank;

public class StarrocksDataTunnelSourceOption extends BaseSourceOption {

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

    @ParamKey("request.retries")
    @OptionDesc("向 StarRocks 发送请求的重试次数")
    private Integer retries = 3;

    @ParamKey("request.connect.timeout.ms")
    @OptionDesc("向 StarRocks 发送请求的连接超时时间")
    private Integer connectTimeout = 30000;

    @ParamKey("request.read.timeout.ms")
    @OptionDesc("向 StarRocks 发送请求的读取超时时间")
    private Integer readTimeout = 30000;

    @ParamKey("request.query.timeout.s")
    @OptionDesc("从 StarRocks 查询数据的超时时间。默认超时时间为 1 小时。-1 表示无超时限制")
    private Integer queryTimeout = 3600;

    @ParamKey("request.tablet.size")
    @OptionDesc("一个 Spark RDD 分区对应的 StarRocks Tablet 的个数。参数设置越小，生成的分区越多，Spark 侧的并行度也就越大，但与此同时会给 StarRocks 侧造成更大的压力")
    private Integer tableSize = Integer.MAX_VALUE;

    @ParamKey("batch.size")
    @OptionDesc("单次从 BE 读取的最大行数。调大参数取值可减少 Spark 与 StarRocks 之间建立连接的次数，从而减轻网络延迟所带来的的额外时间开销。对于StarRocks 2.2及以后版本最小支持的batch size为4096，如果配置小于该值，则按4096处理")
    private Integer batchSize = 4096;

    @ParamKey("exec.mem.limit")
    @OptionDesc("单个查询的内存限制。单位：字节。默认内存限制为 2GB")
    private Long memLimit = 2147483648L;

    @ParamKey("deserialize.arrow.async")
    @OptionDesc("是否支持把 Arrow 格式异步转换为 Spark Connector 迭代所需的 RowBatch")
    private boolean arrowAsync = false;

    @ParamKey("deserialize.queue.size")
    @OptionDesc("异步转换 Arrow 格式时内部处理队列的大小，当 deserialize.arrow.async 为 true 时生效")
    private Integer deserializeQueueSize = 64;

    @ParamKey("filter.query")
    @OptionDesc("指定过滤条件。多个过滤条件用 and 连接。StarRocks 根据指定的过滤条件完成对待读取数据的过滤")
    private String filterQuery;

    @ParamKey("filter.query.in.max.count")
    @OptionDesc("谓词下推中，IN 表达式支持的取值数量上限。如果 IN 表达式中指定的取值数量超过该上限，则 IN 表达式中指定的条件过滤在 Spark 侧处理")
    private Integer filterQueryInMaxCount = 10;


}
