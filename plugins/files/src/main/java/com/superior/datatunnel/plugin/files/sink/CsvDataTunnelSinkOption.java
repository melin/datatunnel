package com.superior.datatunnel.plugin.files.sink;

import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import lombok.Data;

import javax.validation.constraints.NotBlank;

@Data
public class CsvDataTunnelSinkOption extends DataTunnelSinkOption {

    @NotBlank(message = "path can not blank")
    private String path;

    private String sep = ",";

    private String encoding = "UTF-8";

    private String quote = "\"";

    private boolean quoteAll = false;

    private String escape = "\\";

    private boolean escapeQuotes = true;

    private boolean header = false;

    private boolean ignoreLeadingWhiteSpace = true;

    private boolean ignoreTrailingWhiteSpace = true;

    private String nullValue;

    private String dateFormat = "yyyy-MM-dd";

    private String timestampFormat = "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]";

    private String timestampNTZFormat = "yyyy-MM-dd'T'HH:mm:ss[.SSS]";

    private String charToEscapeQuoteEscaping;

    private String emptyValue;

    private String compression;
}
