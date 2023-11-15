package com.superior.datatunnel.plugin.redshift;

import com.superior.datatunnel.api.model.BaseSourceOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@Data
public class RedshiftDataTunnelSourceOption extends BaseSourceOption {

    @OptionDesc("数据库名")
    @NotBlank(message = "databaseName can not blank")
    private String databaseName;

    @OptionDesc("数据库 schema 名")
    private String schemaName;

    @OptionDesc("数据库表名")
    private String tableName;

    private String query;

    @NotEmpty(message = "columns can not empty")
    private String[] columns = new String[]{"*"};

    private String username;

    private String password = "";

    @NotBlank(message = "host can not blank")
    private String host;

    @NotNull(message = "port can not blank")
    private Integer port = 5439;

    @NotNull(message = "region can not blank")
    @OptionDesc("AWS region")
    private String region;

    @OptionDesc("A writeable location in Amazon S3, to be used for unloaded data when reading and Avro data to be loaded into Redshift when writing. " +
            "If you're using Redshift data source for Spark as part of a regular ETL pipeline, it can be useful to set a Lifecycle Policy on a bucket and use that as a temp location for this data.")
    @NotNull(message = "tempdir can not blank")
    private String tempdir;

    @OptionDesc("AWS access key, must have write permissions to the S3 bucket")
    @NotNull(message = "accessKeyId can not blank")
    private String accessKeyId;

    @OptionDesc("AWS secret access key corresponding to provided access key")
    @NotNull(message = "accessKeyId can not blank")
    private String secretAccessKey;

    @OptionDesc("Redshift 关联的 IAM 角色, 需要给该角色添加AK/CK用户ARN sts:AssumeRole 信任, 用于生成临时ak/ck")
    private String redshiftRoleArn;
}
