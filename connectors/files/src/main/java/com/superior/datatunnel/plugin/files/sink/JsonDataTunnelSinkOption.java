package com.superior.datatunnel.plugin.files.sink;

import com.superior.datatunnel.api.model.BaseSinkOption;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import lombok.Data;

import javax.validation.constraints.NotBlank;

@Data
public class JsonDataTunnelSinkOption extends BaseSinkOption {

    @NotBlank(message = "path can not blank")
    private String path;

    private String timeZone;

    private String dateFormat = "yyyy-MM-dd";

    private String timestampFormat = "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]";

    private String timestampNTZFormat = "yyyy-MM-dd'T'HH:mm:ss[.SSS]";

    private String encoding = "UTF-8";

    private boolean ignoreNullFields = false;
}
