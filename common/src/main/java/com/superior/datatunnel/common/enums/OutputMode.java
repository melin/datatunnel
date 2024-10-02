package com.superior.datatunnel.common.enums;

public enum OutputMode {
    APPEND("append"),
    COMPLETE("complete");

    private String name;

    OutputMode(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
