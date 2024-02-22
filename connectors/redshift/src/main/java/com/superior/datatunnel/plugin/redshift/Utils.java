package com.superior.datatunnel.plugin.redshift;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

public class Utils {

    public static Credentials queryCredentials(String accessKeyId, String secretAccessKey, String region, String iamRole) {
        StsClient stsClient = StsClient.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKeyId, secretAccessKey)))
                .httpClient(UrlConnectionHttpClient.builder().build())
                .build();

        AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder()
                .roleArn(iamRole)
                .roleSessionName("DataTunnelRoleSession")
                .build();

        AssumeRoleResponse tokenResponse = stsClient.assumeRole(assumeRoleRequest);
        return tokenResponse.credentials();
    }
}
