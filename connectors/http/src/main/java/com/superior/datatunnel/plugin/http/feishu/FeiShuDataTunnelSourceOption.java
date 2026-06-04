package com.superior.datatunnel.plugin.http.feishu;

import com.superior.datatunnel.api.model.BaseSourceOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import lombok.Data;

@Data
public class FeiShuDataTunnelSourceOption extends BaseSourceOption {

    private String appId;

    private String appSecret;

    @OptionDesc("真实的表格 Token")
    private String spreadsheetToken;

    private String sheetId;
}
