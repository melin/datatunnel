package com.dataworks.datatunnel.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;

import java.util.List;

/****************************************
 * @@CREATE : 2021-08-28 2:04 下午
 * @@AUTH : NOT A CAT【NOTACAT@CAT.ORZ】
 * @@DESCRIPTION :
 * @@VERSION :
 *****************************************/
public class DistCpUtil {

    public static void distcp(Configuration config,
                              List<Path> sourcePaths, Path targetPath,
                              int maxMaps, int mapBandwidth) throws Exception{
        DistCpOptions options = new DistCpOptions(sourcePaths, targetPath);
        options.setMaxMaps(maxMaps);
        options.setMapBandwidth(mapBandwidth);
        options.setOverwrite(true);
        DistCp distcp = new DistCp(config, options);
        distcp.execute();
    }
}
