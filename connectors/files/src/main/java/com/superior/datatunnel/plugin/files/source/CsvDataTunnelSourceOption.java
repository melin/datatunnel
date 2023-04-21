package com.superior.datatunnel.plugin.files.source;

import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import lombok.Data;

import javax.validation.constraints.NotBlank;

@Data
public class CsvDataTunnelSourceOption extends DataTunnelSourceOption {

    @NotBlank(message = "path can not blank")
    private String path;

    private String sep = ",";

    private String encoding = "UTF-8";

    private String quote = "\"";

    private String escape = "\\";

    private String comment;

    private boolean header = false;

    private boolean inferSchema = false;

    private boolean enforceSchema = true;

    private boolean ignoreLeadingWhiteSpace = false;

    private boolean ignoreTrailingWhiteSpace = false;

    private String nullValue;

    private String nanValue = "NaN";

    private String positiveInf = "Inf";

    private String negativeInf = "-Inf";

    private String dateFormat = "yyyy-MM-dd";

    private String timestampFormat = "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]";

    private String timestampNTZFormat = "yyyy-MM-dd'T'HH:mm:ss[.SSS]";

    private int maxColumns = 20480;

    private int maxCharsPerColumn = -1;

    private String mode = "PERMISSIVE";

    private String columnNameOfCorruptRecord;

    private boolean multiLine = false;

    private String charToEscapeQuoteEscaping;

    private double samplingRatio = 1.0;

    private String emptyValue;

    private String locale = "en-US";

    private String unescapedQuoteHandling = "STOP_AT_DELIMITER";
}
