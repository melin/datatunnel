package com.superior.datatunnel.plugin.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

public class HttpDataTunnelSource implements DataTunnelSource {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** repetition 且无 endValue、未开启 autoStop 时的最大请求次数，防止死循环 */
    private static final int REPETITION_MAX_REQUESTS = 10_000;

    private static final String DYNAMIC_REPETITION = "repetition";

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        HttpDataTunnelSourceOption option = (HttpDataTunnelSourceOption) context.getSourceOption();
        if (StringUtils.isBlank(option.getUrl())) {
            throw new DataTunnelException("HTTP source requires url");
        }

        String method =
                StringUtils.defaultIfBlank(option.getMethod(), "GET").trim().toUpperCase();
        if (!"GET".equals(method) && !"POST".equals(method)) {
            throw new DataTunnelException("HTTP source only supports GET and POST, got: " + method);
        }

        String norm = normalizeReqParamType(option.getReqParamType());
        boolean postJsonBody = "POST".equals(method) && ("body".equals(norm) || "json".equals(norm));
        boolean postFormBody = "POST".equals(method)
                && ("form-data".equals(norm) || "formdata".equals(norm) || "x-www-form-urlencoded".equals(norm));

        try {
            List<String> lines;
            if (isRepetitionMode(option)) {
                lines = readAllPages(option, method, postJsonBody, postFormBody);
            } else {
                String bodyJson = null;
                if (postJsonBody) {
                    bodyJson = mergeStaticParamsJson(option.getStaticParams());
                }
                byte[] raw =
                        executeHttp(option, method, postJsonBody, postFormBody, bodyJson, option.getStaticParams());
                lines = extractJsonLines(raw, option);
            }
            if (lines.isEmpty()) {
                return context.getSparkSession().createDataFrame(new ArrayList<Row>(), new StructType());
            }
            Dataset<String> jsonLines = context.getSparkSession().createDataset(lines, Encoders.STRING());
            Dataset<Row> ds = context.getSparkSession().read().json(jsonLines);
            String[] cols = option.getColumns();
            if (cols != null && !(cols.length == 1 && "*".equals(cols[0]))) {
                ds = ds.selectExpr(cols);
            }
            return ds;
        } catch (DataTunnelException e) {
            throw e;
        } catch (Exception e) {
            throw new DataTunnelException("HTTP source failed: " + e.getMessage(), e);
        }
    }

    private static boolean isRepetitionMode(HttpDataTunnelSourceOption option) {
        return StringUtils.equalsIgnoreCase(DYNAMIC_REPETITION, StringUtils.trimToEmpty(option.getDynamicParams()));
    }

    private static List<String> readAllPages(
            HttpDataTunnelSourceOption option, String method, boolean postJsonBody, boolean postFormBody)
            throws Exception {
        String paramName = StringUtils.trimToEmpty(option.getParamName());
        if (StringUtils.isBlank(paramName)) {
            throw new DataTunnelException("HTTP repetition mode requires paramName");
        }
        String startStr = StringUtils.trimToEmpty(option.getStartValue());
        if (StringUtils.isBlank(startStr)) {
            throw new DataTunnelException("HTTP repetition mode requires startValue");
        }
        String stepStr = StringUtils.trimToEmpty(option.getStep());
        if (StringUtils.isBlank(stepStr) || "0".equals(stepStr)) {
            throw new DataTunnelException("HTTP repetition mode requires non-zero step");
        }
        long start = Long.parseLong(startStr);
        long step = Long.parseLong(stepStr);
        String endStr = StringUtils.trimToEmpty(option.getEndValue());
        Long end = StringUtils.isBlank(endStr) ? null : Long.parseLong(endStr);
        boolean autoStop = Boolean.TRUE.equals(option.getAutoStop());
        if (end == null && !autoStop) {
            throw new DataTunnelException(
                    "HTTP repetition mode requires endValue and/or autoStop=true to avoid unbounded requests");
        }

        List<String> allLines = new ArrayList<>();
        long cur = start;
        for (int n = 0; n < REPETITION_MAX_REQUESTS; n++) {
            if (end != null) {
                if (step > 0 && cur > end) {
                    break;
                }
                if (step < 0 && cur < end) {
                    break;
                }
            }
            String staticForReq = injectPageIntoStaticParams(option.getStaticParams(), paramName, cur);
            String bodyJson = null;
            if (postJsonBody) {
                bodyJson = mergeStaticParamsJson(staticForReq);
            }
            byte[] raw = executeHttp(option, method, postJsonBody, postFormBody, bodyJson, staticForReq);
            List<String> batch = extractJsonLines(raw, option);
            if (autoStop && batch.isEmpty()) {
                break;
            }
            allLines.addAll(batch);
            cur += step;
        }
        return allLines;
    }

    private static String injectPageIntoStaticParams(String staticParamsJson, String paramName, long pageValue)
            throws Exception {
        JsonNode n = MAPPER.readTree(mergeStaticParamsJson(staticParamsJson));
        if (!n.isObject()) {
            throw new DataTunnelException("staticParams must be a JSON object when using pagination");
        }
        ObjectNode obj = (ObjectNode) n;
        obj.put(paramName, pageValue);
        return MAPPER.writeValueAsString(obj);
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return HttpDataTunnelSourceOption.class;
    }

    /** Cyber / RestApi：query、body、json、form-data 等，统一成小写且下划线变横杠 */
    private static String normalizeReqParamType(String raw) {
        if (StringUtils.isBlank(raw)) {
            return "query";
        }
        return raw.trim().toLowerCase().replace('_', '-');
    }

    private static byte[] executeHttp(
            HttpDataTunnelSourceOption option,
            String method,
            boolean postJsonBody,
            boolean postFormBody,
            String bodyJson,
            String staticParamsForRequest)
            throws Exception {
        RequestConfig cfg = RequestConfig.custom()
                .setConnectTimeout(30_000)
                .setSocketTimeout(120_000)
                .build();
        try (CloseableHttpClient client =
                HttpClients.custom().setDefaultRequestConfig(cfg).build()) {
            HttpUriRequest req;
            if ("POST".equals(method)) {
                HttpPost post = new HttpPost(option.getUrl());
                applyHeaders(post, option);
                applyAuth(post, option);
                if (postJsonBody && StringUtils.isNotBlank(bodyJson)) {
                    post.setEntity(new StringEntity(bodyJson, ContentType.APPLICATION_JSON));
                } else if (postFormBody) {
                    post.setEntity(buildFormEntity(staticParamsForRequest));
                } else {
                    URI uri = appendQueryParams(URI.create(option.getUrl()), staticParamsForRequest);
                    post.setURI(uri);
                }
                req = post;
            } else {
                URI uri = appendQueryParams(URI.create(option.getUrl()), staticParamsForRequest);
                HttpGet get = new HttpGet(uri);
                applyHeaders(get, option);
                applyAuth(get, option);
                req = get;
            }
            try (CloseableHttpResponse resp = client.execute(req)) {
                int code = resp.getStatusLine().getStatusCode();
                if (code < 200 || code >= 300) {
                    String msg = EntityUtils.toString(resp.getEntity(), StandardCharsets.UTF_8);
                    throw new DataTunnelException("HTTP " + code + ": " + StringUtils.abbreviate(msg, 500));
                }
                byte[] body = EntityUtils.toByteArray(resp.getEntity());
                if (body == null || body.length == 0) {
                    body = "[]".getBytes(StandardCharsets.UTF_8);
                }
                return body;
            }
        }
    }

    private static UrlEncodedFormEntity buildFormEntity(String staticParamsJson) throws Exception {
        List<NameValuePair> pairs = new ArrayList<>();
        if (StringUtils.isNotBlank(staticParamsJson)) {
            JsonNode n = MAPPER.readTree(staticParamsJson);
            if (!n.isObject()) {
                throw new DataTunnelException("form-data requires staticParams to be a JSON object");
            }
            Iterator<Map.Entry<String, JsonNode>> it = n.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                pairs.add(new BasicNameValuePair(e.getKey(), e.getValue().asText()));
            }
        }
        return new UrlEncodedFormEntity(pairs, StandardCharsets.UTF_8);
    }

    private static void applyHeaders(org.apache.http.HttpRequest req, HttpDataTunnelSourceOption option) {
        req.setHeader(HttpHeaders.ACCEPT, "application/json");
        if (StringUtils.isNotBlank(option.getHeaders())) {
            try {
                JsonNode n = MAPPER.readTree(option.getHeaders());
                Iterator<Map.Entry<String, JsonNode>> it = n.fields();
                while (it.hasNext()) {
                    Map.Entry<String, JsonNode> e = it.next();
                    req.setHeader(e.getKey(), e.getValue().asText());
                }
            } catch (Exception ex) {
                throw new DataTunnelException("Invalid headers JSON: " + ex.getMessage());
            }
        }
    }

    private static void applyAuth(org.apache.http.HttpRequest req, HttpDataTunnelSourceOption option) {
        String auth = StringUtils.defaultIfBlank(option.getAuthType(), "none").toLowerCase();
        if ("basic".equals(auth)) {
            String u = StringUtils.defaultString(option.getUsername());
            String p = StringUtils.defaultString(option.getPassword());
            String token = java.util.Base64.getEncoder().encodeToString((u + ":" + p).getBytes(StandardCharsets.UTF_8));
            req.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + token);
        } else if ("token".equals(auth) && StringUtils.isNotBlank(option.getToken())) {
            req.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + option.getToken());
        }
    }

    private static URI appendQueryParams(URI base, String staticParamsJson) throws Exception {
        if (StringUtils.isBlank(staticParamsJson)) {
            return base;
        }
        JsonNode n = MAPPER.readTree(staticParamsJson);
        if (!n.isObject()) {
            return base;
        }
        URIBuilder b = new URIBuilder(base);
        Iterator<Map.Entry<String, JsonNode>> it = n.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            b.addParameter(e.getKey(), e.getValue().asText());
        }
        return b.build();
    }

    private static String mergeStaticParamsJson(String staticParams) {
        return StringUtils.isBlank(staticParams) ? "{}" : staticParams;
    }

    private static List<String> extractJsonLines(byte[] raw, HttpDataTunnelSourceOption option) throws Exception {
        JsonNode root = MAPPER.readTree(raw);
        String resultKey = option.getResultKey();
        JsonNode data = root;
        if (StringUtils.isNotBlank(resultKey)) {
            for (String part : resultKey.split("\\.")) {
                if (data == null || !data.has(part)) {
                    return new ArrayList<>();
                }
                data = data.get(part);
            }
        }

        List<String> lines = new ArrayList<>();
        if (data == null || data.isNull()) {
            return lines;
        }
        if (data.isArray()) {
            for (JsonNode item : data) {
                lines.add(MAPPER.writeValueAsString(item));
            }
        } else if (data.isObject()) {
            if (StringUtils.isBlank(resultKey)) {
                String col = StringUtils.defaultIfBlank(option.getNoMatchColumn(), "payload");
                lines.add("{\"" + col.replace("\"", "\\\"") + "\":" + MAPPER.writeValueAsString(data) + "}");
            } else {
                lines.add(MAPPER.writeValueAsString(data));
            }
        } else {
            lines.add("{\"value\":" + MAPPER.writeValueAsString(data) + "}");
        }
        return lines;
    }
}
