package com.aliyun.odps.cupid.table.v1.writer;

public final class WriteCapabilities {

    private final boolean supportOverwrite;
    private final boolean supportBuckets;
    private final boolean supportDynamicPartition;

    public WriteCapabilities(boolean supportOverwrite, boolean supportBuckets) {
        this(supportOverwrite, supportBuckets, false);
    }

    public WriteCapabilities(boolean supportOverwrite, boolean supportBuckets, boolean supportDynamicPartition) {
        this.supportOverwrite = supportOverwrite;
        this.supportBuckets = supportBuckets;
        this.supportDynamicPartition = supportDynamicPartition;
    }

    public boolean supportBuckets() {
        return supportBuckets;
    }

    public boolean supportOverwrite() {
        return supportOverwrite;
    }

    public boolean supportDynamicPartition() {
        return supportDynamicPartition;
    }
}
