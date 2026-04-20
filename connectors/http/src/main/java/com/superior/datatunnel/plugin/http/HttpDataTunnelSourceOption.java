package com.superior.datatunnel.plugin.http;

import com.superior.datatunnel.api.model.BaseSourceOption;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * HTTP/REST 数据源选项，与 Cyber RestApiSourcePlugin 的 OPTIONS 对齐。
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class HttpDataTunnelSourceOption extends BaseSourceOption {

    /** 请求 URL */
    private String url;

    /** GET / POST */
    private String method = "GET";

    /** JSON 格式请求头，如 {"Authorization":"Bearer x"} */
    private String headers;

    /**
     * 请求参数位置/编码：
     * query — URL 查询串；
     * body / json — POST 时 application/json，正文为 staticParams；
     * form-data / formdata — GET 同 query；POST 时 application/x-www-form-urlencoded，字段来自 staticParams JSON 对象。
     */
    private String reqParamType = "query";

    /** 静态参数 JSON，如 {"page":1,"size":100} */
    private String staticParams;

    /** 从响应 JSON 中取数据的点路径，如 data.list；空则整段为一条记录 */
    private String resultKey;

    /** 当根为对象且无 resultKey 时，列名与 JSON 键不一致时的占位列名 */
    private String noMatchColumn = "payload";

    /** 认证：none / basic / token */
    private String authType = "none";

    /** Basic 用户名 */
    private String username;

    /** Basic 密码 */
    private String password;

    /** Bearer token（Token 认证） */
    private String token;

    /**
     * 分页模式：与 Cyber 一致，取 repetition 时按 paramName/startValue/endValue/step 多次请求并合并结果；空或非 repetition
     * 则只请求一次。
     */
    private String dynamicParams;

    /** 分页参数名，如 pageNum（写入 staticParams 对应键） */
    private String paramName;

    private String startValue;

    /** 可选；空时需 autoStop=true 或依赖 maxPages 安全上限 */
    private String endValue;

    private String step;

    /** 某页解析结果为空时是否停止（与 endValue 二选一或组合使用） */
    private Boolean autoStop;
}
