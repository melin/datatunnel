package com.superior.datatunnel.plugin.s3;

import com.superior.datatunnel.api.model.BaseCommonOption;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import com.superior.datatunnel.common.enums.FileFormat;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Data
public class S3CommonOption extends BaseCommonOption
        implements DataTunnelSourceOption, DataTunnelSinkOption {

    @NotBlank(message = "endpoint can not blank")
    private String endpoint;

    @NotBlank(message = "accessKey can not blank")
    private String accessKey;

    @NotBlank(message = "secretKey can not blank")
    private String secretKey;

    private boolean pathStyleAccess = true;

    private String s3aClientImpl = "org.apache.hadoop.fs.s3a.S3AFileSystem";

    private boolean sslEnabled = false;

    private int connectionTimeout = 600000;

    private String region = "us-east-1";

    @NotNull(message = "format can not null")
    private FileFormat format;

    @NotBlank(message = "filePath can not blank")
    private String filePath;
}
