package com.superior.datatunnel.plugin.redshift;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.util.Properties;
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

public class RedshiftUtils {

    private static final Logger LOG = LoggerFactory.getLogger(RedshiftUtils.class);

    public static Credentials queryCredentials(
            String accessKeyId, String secretAccessKey, String region, String iamRole) {
        StsClient stsClient = StsClient.builder()
                .region(Region.of(region))
                .credentialsProvider(
                        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey)))
                .httpClient(UrlConnectionHttpClient.builder().build())
                .build();

        AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder()
                .roleArn(iamRole)
                .durationSeconds(3600) // 最大只能是 3600
                .roleSessionName("DataTunnelRoleSession")
                .build();

        AssumeRoleResponse tokenResponse = stsClient.assumeRole(assumeRoleRequest);
        return tokenResponse.credentials();
    }

    public static Connection getConnector(String url, String user, String password) {
        try {
            Properties driverProperties = new Properties();
            driverProperties.put("user", user);
            driverProperties.put("password", password);
            DriverRegistry.register("com.amazon.redshift.jdbc42.Driver");
            return DriverManager.getConnection(url, driverProperties);
        } catch (Exception e) {
            LOG.error("redshift queryTableColumnNames failed: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public static String[] queryTableColumnNames(Connection connection, String dbtable) {
        try {
            String sql = "select * from " + dbtable + " where 1 = 0";
            ResultSetMetaData rsmd = connection.prepareStatement(sql).getMetaData();

            int ncols = rsmd.getColumnCount();
            String[] columns = new String[ncols];
            int i = 0;
            while (i < ncols) {
                String columnName = rsmd.getColumnLabel(i + 1);
                columns[i] = columnName;
                i++;
            }
            return columns;
        } catch (Exception e) {
            // 可能写入是表是 preaction 创建的临时表，此时表还不存在
            LOG.error("query columns failed: " + e.getMessage());
            return null;
        }
    }
}
