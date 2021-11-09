package com.dataworker.datax.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;

/**
 * @author melin 2021/7/27 11:48 上午
 */
public class CommonUtils {

    public static String[] parseColumn(String column) {
        JSONArray json = JSON.parseArray(column);
        return json.toArray(new String[0]);
    }
}
