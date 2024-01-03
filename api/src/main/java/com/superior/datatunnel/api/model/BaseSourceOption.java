package com.superior.datatunnel.api.model;

import com.google.common.collect.Maps;
import com.superior.datatunnel.api.DataSourceType;

import javax.validation.constraints.NotEmpty;
import java.util.Map;

public class BaseSourceOption implements DataTunnelSourceOption {

    private DataSourceType dataSourceType;

    private String resultTableName;

    private String cteSql;

    /**
     * 支持表达式(包含：常亮)：您需要按照Source database SQL语法格式，例如["id","table","1","'mingya.wmy'","'null'","to_char(a+1)","2.3","true"] 。
     * 1. id为普通列名。
     * 2. table为包含保留字的列名。
     * 3. 1为整型数字常量。
     * 4. 'mingya.wmy'为字符串常量（注意需要加上一对单引号）。
     * 5. 关于null：
     *      " "表示空。
     *      null表示null。
     *      'null'表示null这个字符串。
     * 6. to_char(a+1)为计算字符串长度函数。
     * 7. 2.3为浮点数。
     * 8. true为布尔值。
     */
    @NotEmpty(message = "columns can not empty")
    private String[] columns = new String[]{"*"};

    /**
     * key 前缀为 properties. 的参数，全部写入 properties
     */
    private final Map<String, String> properties = Maps.newHashMap();

    @Override
    public DataSourceType getDataSourceType() {
        return dataSourceType;
    }

    @Override
    public void setDataSourceType(DataSourceType dataSourceType) {
        this.dataSourceType = dataSourceType;
    }

    @Override
    public String getResultTableName() {
        return resultTableName;
    }

    @Override
    public void setResultTableName(String resultTableName) {
        this.resultTableName = resultTableName;
    }

    @Override
    public String getCteSql() {
        return cteSql;
    }

    @Override
    public void setCteSql(String cteSql) {
        this.cteSql = cteSql;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }
}
