package com.superior.datatunnel.api.model;

public interface DataTunnelSourceOption extends DataTunnelOption {

    String getResultTableName();

    void setResultTableName(String resultTableName);

    String[] getColumns();
}
