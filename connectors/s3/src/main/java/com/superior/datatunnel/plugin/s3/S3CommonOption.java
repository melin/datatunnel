package com.superior.datatunnel.plugin.s3;

import com.superior.datatunnel.api.model.BaseCommonOption;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import lombok.Data;

import javax.validation.constraints.NotBlank;

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

}
