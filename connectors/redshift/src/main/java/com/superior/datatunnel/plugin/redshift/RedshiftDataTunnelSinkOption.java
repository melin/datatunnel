package com.superior.datatunnel.plugin.redshift;

import com.gitee.melin.bee.core.hibernate5.validation.ValidConst;
import com.superior.datatunnel.api.model.BaseSinkOption;
import com.superior.datatunnel.common.annotation.OptionDesc;
import com.superior.datatunnel.common.annotation.SparkConfKey;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@Data
public class RedshiftDataTunnelSinkOption extends BaseSinkOption {

    @NotBlank(message = "databaseName can not blank")
    private String databaseName;

    @NotBlank(message = "tableName can not blank")
    private String tableName;

    @NotEmpty(message = "columns can not empty")
    private String[] columns = new String[]{"*"};

    private String username;

    private String password = "";

    @OptionDesc("A writeable location in Amazon S3, to be used for unloaded data when reading and Avro data to be loaded into Redshift when writing. " +
            "If you're using Redshift data source for Spark as part of a regular ETL pipeline, it can be useful to set a Lifecycle Policy on a bucket and use that as a temp location for this data.")
    private String tempdir;
}
