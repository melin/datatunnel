package com.superior.datatunnel.plugin.log;

import com.superior.datatunnel.api.model.BaseSinkOption;
import lombok.Data;

@Data
public class LogDataTunnelSinkOption extends BaseSinkOption {

    /**
     * Number of rows to show
     */
    private int numRows = 10;

    /**
     * If set to more than 0, truncates strings to truncate characters and all cells will be aligned right.
     */
    private int truncate = 20;

    /**
     * If set to true, prints output rows vertically (one line per column value).
     */
    private boolean vertical = false;
}
