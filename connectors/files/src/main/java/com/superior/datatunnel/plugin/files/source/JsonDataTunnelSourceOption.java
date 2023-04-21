package com.superior.datatunnel.plugin.files.source;

import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import lombok.Data;

import javax.validation.constraints.NotBlank;

@Data
public class JsonDataTunnelSourceOption extends DataTunnelSourceOption {

    @NotBlank(message = "path can not blank")
    private String path;

    private String timeZone;

    private boolean primitivesAsString = false;

    private boolean prefersDecimal = false;

    private boolean allowComments = false;

    private boolean allowUnquotedFieldNames = false;

    private boolean allowSingleQuotes = true;

    private boolean allowNumericLeadingZero = false;

    private boolean allowBackslashEscapingAnyCharacter = false;

    private String mode = "PERMISSIVE";

    private String dateFormat = "yyyy-MM-dd";

    private String timestampFormat = "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]";

    private String timestampNTZFormat = "yyyy-MM-dd'T'HH:mm:ss[.SSS]";

    private boolean multiLine = false;

    private boolean allowUnquotedControlChars = false;

    private String encoding;

    private double samplingRatio = 1.0;

    private boolean dropFieldIfAllNull = false;

    private String locale = "en-US";

    private boolean allowNonNumericNumbers = true;
}
