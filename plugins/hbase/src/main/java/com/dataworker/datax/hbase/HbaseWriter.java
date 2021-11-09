package com.dataworker.datax.hbase;

import com.alibaba.fastjson.JSON;
import com.dataworker.datax.api.DataXException;
import com.dataworker.datax.api.DataxWriter;
import com.dataworker.datax.hbase.constant.MappingMode;
import com.dataworker.datax.hbase.constant.WriteMode;
import com.dataworker.datax.hbase.util.DistCpUtil;
import com.dataworker.spark.jobserver.api.LogUtils;
import com.dazhenyun.hbasesparkproto.HbaseBulkLoadTool;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.spark.HbaseTableMeta;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Objects;

import static com.dataworker.datax.hbase.constant.HbaseWriterOption.*;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class HbaseWriter implements DataxWriter {

    private static final Logger logger = LoggerFactory.getLogger(HbaseWriter.class);

    private static final int STEP_FLAG = 0b1;

    /**
     * 二进制
     * 第一位 1表示生成hfile
     * 第二位 1表示distcp
     * 第三位 1表示load hfile
     * 101和000是不允许的
     */
    private int runningMode = 0b111;

    private String tableName;

    private String hfileDir;

    private WriteMode writeMode = WriteMode.bulkLoad;

    private MappingMode mappingMode = MappingMode.stringConcat;

    private long hfileMaxSize = HConstants.DEFAULT_MAX_FILE_SIZE;

    private long hfileTime = System.currentTimeMillis();

    private boolean compactionExclude = false;

    private String mergeQualifier = "merge";

    private String separator = ",";

    private String distcpHfileDir;

    private int distcpMaxMaps = 5;

    private int distcpPerMapBandwidth = 100;

    private Date bizdate = new Date();

    @Override
    public void validateOptions(Map<String, String> options) {
        logger.debug("HbaseWriter options={}", JSON.toJSONString(options));
        String runningMode = options.get(RUNNING_MODE);
        if (!Objects.isNull(runningMode)){
            try {
                this.runningMode = Integer.parseInt(runningMode, 2);
            } catch (Exception e) {
                throw new DataXException("runningMode参数错误");
            }
        }

        if (this.runningMode > 0b111 || this.runningMode == 0b101 || this.runningMode == 0b000){
            throw new DataXException("runningMode参数错误");
        }
        // 暂时只支持这两种模式
        if (this.runningMode != 0b111 && this.runningMode != 0b110){
            throw new DataXException("runningMode参数错误");
        }

        if (haveCreateHfileStep()){
            validateCreateHfileOptions(options);
        }

        if (haveDistcpStep()){
            validateDistcpOptions(options);
        }

        if (haveLoadStep()){
            validateLoadOptions(options);
        }
        logger.info("init HbaseWriter={}", this);
    }

    public void validateCreateHfileOptions(Map<String, String> options){
        String tableName = options.get(TABLE_NAME);
        if (StringUtils.isBlank(tableName)){
            throw new DataXException("缺少tableName参数");
        }
        this.tableName = tableName;

        String hfileDir = options.get(HFILE_DIR);
        if (StringUtils.isBlank(hfileDir)){
            throw new DataXException("缺少hfileDir参数");
        }
        this.hfileDir = hfileDir;

        WriteMode writeMode = EnumUtils.getEnum(WriteMode.class, options.get(WRITE_MODE));
        if (!Objects.isNull(writeMode)){
            this.writeMode = writeMode;
        }

        MappingMode mappingMode = EnumUtils.getEnum(MappingMode.class, options.get(MAPPING_MODE));
        if (!Objects.isNull(mappingMode)){
            this.mappingMode = mappingMode;
        }
        if (MappingMode.arrayZstd.equals(this.mappingMode) || MappingMode.stringConcat.equals(this.mappingMode)){
            String mergeQualifier = options.get(MERGE_QUALIFIER);
            if (StringUtils.isNotBlank(mergeQualifier)){
                this.mergeQualifier = mergeQualifier;
            }
        }
        if (MappingMode.stringConcat.equals(this.mappingMode)){
            String separator = options.get(SEPARATOR);
            if (StringUtils.isNotEmpty(separator)){
                this.separator = separator;
            }
        }

        String hfileMaxSize = options.get(HFILE_MAX_SIZE);
        if (!Objects.isNull(hfileMaxSize)){
            try {
                this.hfileMaxSize = Long.valueOf(hfileMaxSize);
            } catch (Exception e) {
                throw new DataXException("hfileMaxSize参数错误");
            }
        }
        if (this.hfileMaxSize <= 0){
            throw new DataXException("hfileMaxSize参数错误");
        }

        String hfileTime = options.get(HFILE_TIME);
        if (!Objects.isNull(hfileTime)){
            try {
                this.hfileTime = Long.valueOf(hfileTime);
            } catch (Exception e) {
                throw new DataXException("hfileTime参数错误");
            }
        }

        String compactionExclude = options.get(COMPACTION_EXCLUDE);
        if (!Objects.isNull(compactionExclude)){
            try {
                this.compactionExclude = Boolean.valueOf(compactionExclude);
            } catch (Exception e) {
                throw new DataXException("compactionExclude参数错误,支持true或false");
            }
        }
    }

    public void validateDistcpOptions(Map<String, String> options){
        String distHfileDir = options.get(DISTCP_HFILE_DIR);
        if (StringUtils.isNotBlank(distHfileDir)){
            this.distcpHfileDir = distHfileDir;
        } else {
            this.distcpHfileDir = this.hfileDir;
        }

        String distcpMaxMaps = options.get(DISTCP_MAX_MAPS);
        if (!Objects.isNull(distcpMaxMaps)){
            try {
                this.distcpMaxMaps = Integer.valueOf(distcpMaxMaps);
            } catch (Exception e) {
                throw new DataXException("distcp.maxMaps参数配置错误");
            }
        }
        if (this.distcpMaxMaps <= 0){
            throw new DataXException("distcp.maxMaps参数配置错误");
        }

        String distcpPerMapBandwidth = options.get(DISTCP_PER_MAP_BANDWIDTH);
        if (!Objects.isNull(distcpPerMapBandwidth)){
            try {
                this.distcpPerMapBandwidth = Integer.valueOf(distcpPerMapBandwidth);
            } catch (Exception e) {
                throw new DataXException("distcp.perMapBandwidth参数配置错误");
            }
        }
        if (this.distcpPerMapBandwidth <= 0){
            throw new DataXException("distcp.perMapBandwidth参数配置错误");
        }
    }

    public void validateLoadOptions(Map<String, String> options){

    }

    @Override
    public void write(SparkSession sparkSession, Dataset<Row> dataset, Map<String, String> options) throws IOException {
        // 实例编号
        String jobInstanceCode = sparkSession.sparkContext().getConf().get("spark.datawork.job.code");
        if (StringUtils.isBlank(jobInstanceCode)){
            throw new DataXException("实例编号为空");
        }

        long count = dataset.count();
        logger.info("dataset count={}", count);
        LogUtils.info(sparkSession, "dataset count=" + count);
        if (0 == count){
            throw new DataXException("dataset为空");
        }

        logger.info("开始hbaseWriter");
        LogUtils.info(sparkSession, "开始hbaseWriter");

        if (haveCreateHfileStep()){
            createHfileStep(sparkSession, dataset, jobInstanceCode);
        }

        if (haveDistcpStep()){
            distcpStep(sparkSession, jobInstanceCode);
        }

        if (haveLoadStep()){
            loadStep(sparkSession, jobInstanceCode);
        }

        logger.info("hbaseWriter成功");
        LogUtils.info(sparkSession, "hbaseWriter成功");
    }

    public boolean haveCreateHfileStep(){
        return 1 == (this.runningMode >> 2 & STEP_FLAG);
    }

    public boolean haveDistcpStep(){
        return 1 == (this.runningMode >> 1 & STEP_FLAG);
    }

    public boolean haveLoadStep(){
        return 1 == (this.runningMode & STEP_FLAG);
    }

    public void createHfileStep(SparkSession sparkSession, Dataset<Row> dataset, String jobInstanceCode) throws IOException{
        logger.info("开始createHfileStep");
        LogUtils.info(sparkSession, "开始createHfileStep");
        Configuration destConfig = getDestConfig();
        Connection destConnection = ConnectionFactory.createConnection(destConfig);

        String tmpDir = buildTmpDir(hfileDir, jobInstanceCode);
        HbaseTableMeta hbaseTableMeta = null;
        try {
            hbaseTableMeta = HbaseBulkLoadTool.buildHbaseTableMeta(destConnection, tableName, tmpDir);
            logger.info("hbaseTableMeta={}", hbaseTableMeta);
            LogUtils.info(sparkSession, "hbaseTableMeta=" + hbaseTableMeta);
        } catch (Exception e) {
            logger.error("创建hbaseTableMeta失败", e);
            LogUtils.error(sparkSession, "创建hbaseTableMeta失败" + e.getMessage());
            throw new DataXException("jobInstanceCode=" + jobInstanceCode + " 创建hbaseTableMeta失败", e);
        } finally {
            if (!destConnection.isClosed()){
                destConnection.close();
            }
        }

        Configuration sourceConfig = getSourceConfig();
        // hfile路径
        String stagingDir = hbaseTableMeta.getStagingDir();
        logger.info("hfile路径={}", stagingDir);
        LogUtils.info(sparkSession, "hfile路径=" + stagingDir);
        // hfile生成成功后的路径
        String stagingDirSucc = buildStagingDirSucc(stagingDir);
        Path stagingDirPath = new Path(stagingDir);
        Path stagingDirSuccPath = new Path(stagingDirSucc);

        // 判断目录是否存在
        FileSystem fileSystem = FileSystem.get(sourceConfig);
        try {
            if (fileSystem.exists(stagingDirPath)){
                fileSystem.delete(stagingDirPath, true);
                logger.warn("目录{}已存在,清空目录", stagingDir);
                LogUtils.warn(sparkSession, "目录" + stagingDir + "已存在,清空目录");
            }
            if (fileSystem.exists(stagingDirSuccPath)){
                fileSystem.delete(stagingDirSuccPath, true);
                logger.warn("目录{}已存在,清空目录", stagingDirSucc);
                LogUtils.warn(sparkSession, "目录" + stagingDirSucc + "已存在,清空目录");
            }
        } catch (Exception e) {
            logger.error("清空目录异常", e);
            LogUtils.error(sparkSession, "清空目录异常");
            throw new DataXException("jobInstanceCode=" + jobInstanceCode + "清空目录异常", e);
        }

        int regionSize = hbaseTableMeta.getStartKeys().length;
        logger.info("table={} regionSize={}", tableName, regionSize);
        LogUtils.info(sparkSession, "table=" + tableName + " regionSize=" + regionSize);

        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        JavaHBaseContext javaHBaseContext = new JavaHBaseContext(jsc, sourceConfig);

        try {
            if (WriteMode.bulkLoad.equals(writeMode)){
                HbaseBulkLoadTool.dazhenBulkLoad(javaHBaseContext,
                        dataset,
                        hbaseTableMeta,
                        mappingMode.createBulkLoadFunction(hbaseTableMeta.getColumnFamily(), mergeQualifier, separator),
                        compactionExclude,
                        hfileMaxSize,
                        hfileTime,
                        null
                );
            } else {
                HbaseBulkLoadTool.dazhenBulkLoadThinRows(javaHBaseContext,
                        dataset,
                        hbaseTableMeta,
                        mappingMode.createThinBulkLoadFunction(hbaseTableMeta.getColumnFamily(), mergeQualifier, separator),
                        compactionExclude,
                        hfileMaxSize,
                        hfileTime,
                        null
                );
            }
            logger.info("writeMode={},mappingMode={} hfile生成成功", writeMode, mappingMode);
            LogUtils.info(sparkSession, "writeMode=" + writeMode + " mappingMode=" + mappingMode + " hfile生成成功");
        } catch (Exception e) {
            logger.error("hfile生成失败", e);
            LogUtils.error(sparkSession, "hfile生成失败," + e.getMessage());
            // 清理目录
            fileSystem.delete(stagingDirPath, true);
            LogUtils.error(sparkSession, "清理目录" + stagingDirPath);
            throw new DataXException("jobInstanceCode=" + jobInstanceCode + " hfile生成失败", e);
        }

        // hifle目录改成_succ后缀
        if (fileSystem.rename(stagingDirPath, stagingDirSuccPath)){
            logger.info("修改hfile目录名={}成功", stagingDirSuccPath);
            LogUtils.info(sparkSession, "修改hfile目录名=" + stagingDirSuccPath + "成功");
        } else {
            logger.error("修改hfile目录名={}失败", stagingDirSuccPath);
            LogUtils.error(sparkSession, "修改hfile目录名=" + stagingDirSuccPath + "失败");
            throw new DataXException("jobInstanceCode=" + jobInstanceCode + " 修改hfile目录名=" + stagingDirSuccPath + "失败");
        }

        // 统计
        try {
            statisticsHfileInfo(sparkSession, sourceConfig, stagingDirSuccPath);
        } catch (Exception e) {
            logger.warn("统计hfile信息异常", e);
            LogUtils.warn(sparkSession, "统计hfile信息异常");
        }
        logger.info("结束createHfileStep");
        LogUtils.info(sparkSession, "结束createHfileStep");
    }

    public void distcpStep(SparkSession sparkSession, String jobInstanceCode){
        logger.info("开始distcpStep");
        LogUtils.info(sparkSession, "开始distcpStep");

        try {
            Configuration sourceConfig = getSourceConfig();
            Configuration destConfig = getDestConfig();

            String destTmpDir = buildTmpDir(distcpHfileDir, jobInstanceCode);
            String destStagingDir = HbaseBulkLoadTool.buildStagingDir(destTmpDir);
            logger.info("distCp目录={}", destStagingDir);
            LogUtils.info(sparkSession, "distCp目录=" + destStagingDir);
            UserGroupInformation operateUser = UserGroupInformation.getCurrentUser();
            if (StringUtils.isBlank(destConfig.get("hadoop.security.authentication")) || "simple".equals(destConfig.get("hadoop.security.authentication"))) {
                operateUser = UserGroupInformation.createRemoteUser("admin");
            }

            String nameServices = sourceConfig.get("dfs.nameservices");
            logger.info("nameServices={}", nameServices);
            LogUtils.info(sparkSession, "nameServices=" + nameServices);
            String[] nameServiceArray = nameServices.split(",");

            String sourceStagingDir = HbaseBulkLoadTool.buildStagingDir(buildTmpDir(hfileDir, jobInstanceCode));
            String sourceStagingDirSucc = buildStagingDirSucc(sourceStagingDir);
            Path sourceStagingDirSuccPath = new Path(sourceStagingDirSucc);

            Path sourcePath = new Path("hdfs://" + nameServiceArray[0] + sourceStagingDirSuccPath);
            Path destPath = new Path("hdfs://" + nameServiceArray[1] + destStagingDir);

            logger.info("sourcePath={},destPath={}", sourcePath, destPath);
            LogUtils.info(sparkSession, "sourcePath=" + sourcePath + ",destPath=" + destPath);

            DistCpUtil.distcp(sourceConfig, Arrays.asList(sourcePath), destPath, distcpMaxMaps, distcpPerMapBandwidth);

            //distcp成功后创建distcp.succ文件
            operateUser.doAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    FileSystem destFileSystem = FileSystem.get(destConfig);
                    destFileSystem.create(new Path(destStagingDir, "distcp.succ"));
                    return null;
                }
            });

            logger.info("distcp成功");
            LogUtils.info(sparkSession, "distcp成功");
            // 删除原集群数据
            FileSystem.get(sourceConfig).delete(sourceStagingDirSuccPath, true);
            logger.info("删除原集群hfile成功");
            LogUtils.info(sparkSession, "删除原集群hfile成功");
        } catch (Exception e){
            logger.error("distcp失败", e);
            LogUtils.error(sparkSession, "distcp失败");
            throw new DataXException("jobInstanceCode=" + jobInstanceCode + " distcp失败", e);
        }
        logger.info("结束distcpStep");
        LogUtils.info(sparkSession, "结束distcpStep");
    }

    public void loadStep(SparkSession sparkSession, String jobInstanceCode){
        logger.info("开始loadStep");
        LogUtils.info(sparkSession, "开始loadStep");

        try {
            Configuration destConfig = getDestConfig();
            UserGroupInformation operateUser = UserGroupInformation.getCurrentUser();
            if (StringUtils.isBlank(destConfig.get("hadoop.security.authentication")) || "simple".equals(destConfig.get("hadoop.security.authentication"))) {
                operateUser = UserGroupInformation.createRemoteUser("admin");
            }
            String destTmpDir = buildTmpDir(distcpHfileDir, jobInstanceCode);
            operateUser.doAs(new PrivilegedExceptionAction<Void>(){
                @Override
                public Void run() throws Exception {
                    HbaseBulkLoadTool.loadIncrementalHFiles(ConnectionFactory.createConnection(destConfig), tableName, destTmpDir);
                    return null;
                }
            });
        } catch (Exception e) {
            logger.error("load失败", e);
            LogUtils.error(sparkSession, "load失败");
            throw new DataXException("jobInstanceCode=" + jobInstanceCode + " load失败", e);

        }
        logger.info("结束loadStep");
        LogUtils.info(sparkSession, "结束loadStep");
    }

    private Configuration getSourceConfig(){
        Configuration config = new Configuration();
        config.addResource(Thread.currentThread().getContextClassLoader().getResource("source" + "/core-site.xml"));
        config.addResource(Thread.currentThread().getContextClassLoader().getResource("source" + "/yarn-site.xml"));
        config.addResource(Thread.currentThread().getContextClassLoader().getResource("source" + "/mapred-site.xml"));
        config.addResource(Thread.currentThread().getContextClassLoader().getResource("source" + "/hdfs-site.xml"));
        //https://blog.csdn.net/qq_26838315/article/details/111941281
        //安全集群和非安全集群之间进行数据迁移时需要配置参数ipc.client.fallback-to-simple-auth-allowed 为 true
        config.set("ipc.client.fallback-to-simple-auth-allowed", "true");
        return config;
    }

    private Configuration getDestConfig(){
        Configuration config = new Configuration(false);
        config.addResource(Thread.currentThread().getContextClassLoader().getResource("dest" + "/core-site.xml"));
        config.addResource(Thread.currentThread().getContextClassLoader().getResource("dest" + "/hdfs-site.xml"));
        config.addResource(Thread.currentThread().getContextClassLoader().getResource("dest" + "/hbase-site.xml"));
        return config;
    }

    private String buildTmpDir(String stagingDir, String jobInstanceCode){
        StringBuilder sb = new StringBuilder();
        sb.append(LocalDateTime.ofInstant(bizdate.toInstant(), ZoneId.systemDefault()).toLocalDate());
        if (stagingDir.startsWith("/")){
            sb.append(stagingDir);
        } else {
            sb.append("/").append(stagingDir);
        }
        if (!stagingDir.endsWith("/")){
            sb.append("/");
        }

        sb.append(jobInstanceCode);
        return sb.toString();
    }

    private String buildStagingDirSucc(String stagingDir){
        return stagingDir + "_succ";
    }

    /**
     * 统计hfile信息
     * @param sparkSession
     * @param config
     * @param path
     * @throws IOException
     */
    private void statisticsHfileInfo(SparkSession sparkSession, Configuration config, Path path) throws Exception{
        StringBuilder sb = new StringBuilder("hfile生成成功, 临时路径：" + path + ":\n");
        long totalSize = 0;
        FileSystem fileSystem = FileSystem.get(config);
        FileStatus[] parentFileStatus = fileSystem.listStatus(path);
        for (FileStatus parent : parentFileStatus){
            FileStatus[] childFileStatus = fileSystem.listStatus(parent.getPath());
            for (int i = 0; i < childFileStatus.length; i++){
                sb.append("hfile_" + i + ":" + FileUtils.byteCountToDisplaySize(childFileStatus[i].getLen()) + "\n");
                totalSize += childFileStatus[i].getLen();
            }
        }
        sb.append("hfile总大小:" + FileUtils.byteCountToDisplaySize(totalSize));
        logger.info(sb.toString());
        LogUtils.info(sparkSession, sb.toString());
    }

    @Override
    public String toString() {
        return "HbaseWriter{" +
                "runningMode=" + Integer.toBinaryString(runningMode) +
                ", tableName='" + tableName + '\'' +
                ", hfileDir='" + hfileDir + '\'' +
                ", writeMode=" + writeMode +
                ", mappingMode=" + mappingMode +
                ", hfileMaxSize=" + hfileMaxSize +
                ", hfileTime=" + hfileTime +
                ", compactionExclude=" + compactionExclude +
                ", mergeQualifier='" + mergeQualifier + '\'' +
                ", separator='" + separator + '\'' +
                ", distcpHfileDir='" + distcpHfileDir + '\'' +
                ", distcpMaxMaps=" + distcpMaxMaps +
                ", distcpPerMapBandwidth=" + distcpPerMapBandwidth +
                ", bizdate=" + bizdate +
                '}';
    }
}
