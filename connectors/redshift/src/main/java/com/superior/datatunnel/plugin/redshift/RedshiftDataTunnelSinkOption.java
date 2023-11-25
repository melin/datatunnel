package com.superior.datatunnel.plugin.redshift;

import com.superior.datatunnel.api.model.BaseSinkOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import com.superior.datatunnel.common.enums.WriteMode;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Data
public class RedshiftDataTunnelSinkOption extends BaseSinkOption {

    @OptionDesc("数据库名")
    private String databaseName;

    @OptionDesc("数据库 schema 名")
    @NotBlank(message = "schemaName can not blank")
    private String schemaName;

    @OptionDesc("数据库表名")
    @NotBlank(message = "tableName can not blank")
    private String tableName;

    private String username;

    private String password = "";

    private String host;

    private Integer port = 5439;

    @OptionDesc("jdbc 连接地址，如果填写jdbcUrl, 就不需要填写host & port")
    private String jdbcUrl;

    @OptionDesc("数据写入模式")
    @NotNull(message = "writeMode can not null")
    private WriteMode writeMode = WriteMode.APPEND;

    @NotNull(message = "region can not blank")
    private String region;

    @OptionDesc("A writeable location in Amazon S3, to be used for unloaded data when reading and Avro data to be loaded into Redshift when writing. " +
            "If you're using Redshift data source for Spark as part of a regular ETL pipeline, it can be useful to set a Lifecycle Policy on a bucket and use that as a temp location for this data.")
    @NotNull(message = "tempdir can not blank")
    private String tempdir;

    @OptionDesc("AWS access key, must have write permissions to the S3 bucket")
    @NotNull(message = "accessKeyId can not blank")
    private String accessKeyId;

    @OptionDesc("AWS secret access key corresponding to provided access key")
    @NotNull(message = "secretAccessKey can not blank")
    private String secretAccessKey;

    @OptionDesc("Redshift 关联的 IAM 角色, 需要给该角色添加AK/CK用户ARN sts:AssumeRole 信任")
    @NotNull(message = "redshiftRoleArn can not blank")
    private String redshiftRoleArn;

    @OptionDesc("Redshift 关联的 IAM 角色, 需要给该角色添加AK/CK用户ARN sts:AssumeRole 信任")
    private String preactions;

    private String postactions;
}
