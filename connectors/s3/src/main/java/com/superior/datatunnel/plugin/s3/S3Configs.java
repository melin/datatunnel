package com.superior.datatunnel.plugin.s3;

import lombok.val;

abstract public class S3Configs {
    public static final String awsServicesEnableV4 = "com.amazonaws.services.s3.enableV4";
    public static final String accessId = "fs.s3a.access.key";
    public static final String secretKey = "fs.s3a.secret.key";
    public static final String pathStyleAccess = "fs.s3a.path.style.access";
    public static final String endPoint = "fs.s3a.endpoint";
    public static final String s3aClientImpl = "fs.s3a.impl";
    public static final String sslEnabled = "fs.s3a.connection.ssl.enabled";
}
