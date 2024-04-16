package com.superior.datatunnel.plugin.s3;

public abstract class S3Configs {
    // aws
    public static final String AWS_ACCESS_KEY = "fs.s3a.access.key";

    public static final String AWS_SECRET_KEY = "fs.s3a.secret.key";

    public static final String AWS_ENDPOINT = "fs.s3a.endpoint";

    public static final String AWS_S3A_CLIENT_IMPL = "fs.s3a.impl";

    public static final String AWS_REGION = "fs.s3a.endpoint.region";

    // oss
    public static final String OSS_ACCESS_KEY = "fs.oss.accessKeyId";

    public static final String OSS_SECRET_KEY = "fs.oss.accessKeySecret";

    public static final String OSS_ENDPOINT = "fs.oss.endpoint";

    public static final String OSS_S3A_CLIENT_IMPL = "fs.oss.impl";
}
