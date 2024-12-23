package com.superior.datatunnel.common.util;

import com.gitee.melin.bee.core.extension.ExtensionLoader;
import com.superior.datatunnel.api.DataSourceType;
import com.superior.datatunnel.api.DataTunnelSink;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.DistCpAction;

public class DatatunnelUtils {

    public static DataTunnelSource getSourceConnector(DataSourceType sourceType) {

        ExtensionLoader<DataTunnelSource> readLoader = ExtensionLoader.getExtensionLoader(DataTunnelSource.class);
        return readLoader.getExtension(sourceType.name().toLowerCase());
    }

    public static DataTunnelSink getSinkConnector(DataSourceType sinkType) {

        ExtensionLoader<DataTunnelSink> readLoader = ExtensionLoader.getExtensionLoader(DataTunnelSink.class);
        return readLoader.getExtension(sinkType.name().toLowerCase());
    }

    public static DistCpAction getDistCpAction() {
        ExtensionLoader<DistCpAction> readLoader = ExtensionLoader.getExtensionLoader(DistCpAction.class);
        return readLoader.getExtension("distcp");
    }
}
