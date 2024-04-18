package com.superior.datatunnel.api.model;

public interface DataTunnelSourceOption extends DataTunnelOption {

    String getSourceTempView();

    void setSourceTempView(String sourceTempView);

    String[] getColumns();
}
