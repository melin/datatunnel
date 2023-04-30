package com.superior.datatunnel.plugin.elasticsearch.enums;

public enum VersionType {
    INTERNAL("internal"), //
    EXTERNAL("external"), //
    EXTERNAL_GTE("external_gte"), //
    /**
     * @since 4.4
     */
    FORCE("force");

    private final String esName;

    VersionType(String esName) {
        this.esName = esName;
    }

    public String getEsName() {
        return esName;
    }

}
