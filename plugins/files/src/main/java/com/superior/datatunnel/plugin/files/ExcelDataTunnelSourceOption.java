package com.superior.datatunnel.plugin.files;

import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import lombok.Data;

import javax.validation.constraints.NotBlank;

@Data
public class ExcelDataTunnelSourceOption extends DataTunnelSourceOption {

    @NotBlank(message = "path can not blank")
    private String path;

    private String schema;

    private String dataAddress = "A1";

    private boolean header = true;

    private boolean treatEmptyValuesAsNulls = true;

    private boolean setErrorCellsToFallbackValues = false;

    private boolean usePlainNumberFormat = false;

    private boolean inferSchema = false;

    private boolean addColorColumns = false;

    private String timestampFormat = "yyyy-MM-dd hh:mm:ss";

    private int maxRowsInMemory = 200;

    private int maxByteArraySize = 0;

    private int tempFileThreshold = 0;

    private int excerptSize = 10;

    private String workbookPassword;
}
