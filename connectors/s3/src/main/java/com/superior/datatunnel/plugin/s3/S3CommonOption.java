package com.superior.datatunnel.plugin.s3;

import com.superior.datatunnel.api.model.BaseCommonOption;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import com.superior.datatunnel.common.enums.FileFormat;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@Data
public class S3CommonOption extends BaseCommonOption
        implements DataTunnelSourceOption, DataTunnelSinkOption {

    private String endpoint;

    private String region;

    private String accessKey;

    private String secretKey;

    private String fsClientImpl = "org.apache.hadoop.fs.s3a.S3AFileSystem";

    private boolean sslEnabled = false;

    private int connectionTimeout = 600000;

    @NotNull(message = "format can not null")
    private FileFormat format;

    @NotBlank(message = "filePath can not blank")
    private String filePath;

    @OptionDesc("csv 字段分隔符")
    private String sep = ",";

    @OptionDesc("csv 文件编码")
    private String encoding = "UTF-8";

    @OptionDesc("csv 文件，第一行是否为字段名")
    private boolean header = false;

    @NotEmpty(message = "columns can not empty")
    private String[] columns = new String[]{"*"};

    @Override
    public String[] getColumns() {
        return columns;
    }
}
