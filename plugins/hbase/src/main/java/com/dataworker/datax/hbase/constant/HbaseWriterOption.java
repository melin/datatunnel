package com.dataworker.datax.hbase.constant;

/****************************************
 * @@CREATE : 2021-08-26 9:25 上午
 * @@AUTH : NOT A CAT【NOTACAT@CAT.ORZ】
 * @@DESCRIPTION : habse bulkload参数配置
 * @@VERSION :
 *
 *****************************************/
public class HbaseWriterOption {

    /**
     * hbase表名
     */
    public static final String TABLE ="table";

    /**
     * bulkLoad的模式
     */
    public static final String WRITE_MODE ="writeMode";

    /**
     * 字段映射模式
     */
    public static final String MAPPING_MODE ="mappingMode";

    /**
     * 生成的hfile的路径
     */
    public static final String HFILE_DIR ="hfileDir";

    /**
     * 生成hfile的时间
     */
    public static final String HFILE_TIME ="hfileTime";

    /**
     * 生成hfile的最大大小
     */
    public static final String HFILE_MAX_SIZE = "hfileMaxSize";

    /**
     * 生成hfile后是否bulkLoad
     */
    public static final String DO_BULKLOAD ="doBulkLoad";

    /**
     * mappingMode模式下,合并字段的列名
     */
    public static final String MERGE_QUALIFIER = "mergeQualifier";

    /**
     * the exclude compaction metadata flag for the HFile
     */
    public static final String COMPACTION_EXCLUDE = "compactionExclude";

    /**
     * distcp 目录
     */
    public static final String DISTCP_HFILE_DIR = "distcp.hfileDir";


    /**
     * distcp最大map数量
     */
    public static final String DISTCP_MAXMAPS = "distcp.maxMaps";

    /**
     * distcp每个map最大带宽
     */
    public static final String DISTCP_MAPBANDWIDTH = "distcp.mapBandwidth";




}
