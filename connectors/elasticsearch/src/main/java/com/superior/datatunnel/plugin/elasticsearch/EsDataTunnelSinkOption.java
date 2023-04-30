package com.superior.datatunnel.plugin.elasticsearch;

import com.superior.datatunnel.api.model.BaseSinkOption;
import com.superior.datatunnel.plugin.elasticsearch.enums.Dynamic;
import com.superior.datatunnel.plugin.elasticsearch.enums.VersionType;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import java.util.List;

@Data
public class EsDataTunnelSinkOption extends BaseSinkOption {

    @NotBlank(message = "hosts can not blank")
    private String hosts;

    @NotBlank(message = "resource can not blank")
    private String resource;

    @NotBlank(message = "indexName can not blank")
    private String indexName;

    private String idField;

    private VersionType versionType = VersionType.EXTERNAL;

    private Dynamic dynamic = Dynamic.INHERIT;

    private List<FieldMeta> fields;
}
