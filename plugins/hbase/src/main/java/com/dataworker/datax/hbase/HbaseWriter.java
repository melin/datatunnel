package com.dataworker.datax.hbase;

import com.alibaba.fastjson.JSON;
import com.dataworker.datax.api.DataXException;
import com.dataworker.datax.api.DataxWriter;
import com.dataworker.datax.hbase.constant.MappingMode;
import com.dataworker.datax.hbase.constant.WriteMode;
import com.dataworker.datax.hbase.util.DistCpUtil;
import com.dataworker.spark.jobserver.api.LogUtils;
import com.dazhenyun.hbasesparkproto.HbaseBulkLoadTool;
import com.dazhenyun.hbasesparkproto.function.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.spark.HbaseTableMeta;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.dataworker.datax.hbase.constant.HbaseWriterOption.*;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class HbaseWriter implements DataxWriter {

    private static final Logger logger = LoggerFactory.getLogger(HbaseWriter.class);

    /**
     * mappingMode模式下,合并字段的默认列名
     */
    public static final String DEFAULT_MERGE_QUALIFIER = "merge";

    /**
     * 默认hfile的最大大小
     */
    public static final long DEFAULT_HFILE_MAX_SIZE = HConstants.DEFAULT_MAX_FILE_SIZE * 5;

    @Override
    public void validateOptions(Map<String, String> options) {
        logger.debug("HbaseWriter options={}", JSON.toJSONString(options));

        if (StringUtils.isBlank(options.get(TABLE))){
            throw new DataXException("缺少table参数");
        }

        if (StringUtils.isBlank(options.get(HFILE_DIR))){
            throw new DataXException("缺少hfileDir参数");
        }

        if (Objects.isNull(EnumUtils.getEnum(WriteMode.class, options.get(WRITE_MODE)))){
//            现只提供bulkLoad模式
//            throw new DataXException("writeMode参数错误,支持bulkLoad或thinBulkLoad");
            options.put(WRITE_MODE, WriteMode.bulkLoad.name());
        }

        if (Objects.isNull(EnumUtils.getEnum(MappingMode.class, options.get(MAPPING_MODE)))){
//            现只提供one2one模式
//            throw new DataXException("mappingMode参数错误,支持one2one或arrayZstd");
            options.put(MAPPING_MODE, MappingMode.one2one.name());
        }

        if (Objects.isNull(BooleanUtils.toBooleanObject(options.get(DO_BULKLOAD)))){
            throw new DataXException("doBulkLoad参数错误,支持true或false");
        }

        if (!Objects.isNull(options.get(HFILE_MAX_SIZE))){
            if (NumberUtils.toLong(options.get(HFILE_MAX_SIZE)) <= 0){
                throw new DataXException("hfileMaxSize参数错误");
            }
        }

        if (!Objects.isNull(options.get(HFILE_TIME))){
            if (!NumberUtils.isCreatable(HFILE_TIME)){
                throw new DataXException("hfileTime参数错误");
            }
        }

        if (BooleanUtils.toBooleanObject(options.get(DO_BULKLOAD))){
            if (NumberUtils.toInt(options.get(DISTCP_MAXMAPS)) <= 0){
                throw new DataXException("distcp.maxMaps参数未配置或配置错误");
            }
            if (NumberUtils.toInt(options.get(DISTCP_MAPBANDWIDTH)) <= 0){
                throw new DataXException("distcp.mapBandwidth参数未配置或配置错误");
            }
        }
    }

    @Override
    public void write(SparkSession sparkSession, Dataset<Row> dataset, Map<String, String> options) throws IOException {
        //实例编号
        String jobInstanceCode = sparkSession.sparkContext().getConf().get("spark.datawork.job.code");
        if (StringUtils.isBlank(jobInstanceCode)){
            throw new DataXException("实例编号为空");
        }

        if (0 == dataset.count()){
            throw new DataXException("dataset为空");
        }
        logger.info("开始hbaseWriter");
        LogUtils.info(sparkSession,"开始hbaseWriter");
        Configuration sourceConfig = new Configuration();
        sourceConfig.addResource(Thread.currentThread().getContextClassLoader().getResource("source" + "/core-site.xml"));
        sourceConfig.addResource(Thread.currentThread().getContextClassLoader().getResource("source" + "/yarn-site.xml"));
        sourceConfig.addResource(Thread.currentThread().getContextClassLoader().getResource("source" + "/mapred-site.xml"));
        sourceConfig.addResource(Thread.currentThread().getContextClassLoader().getResource("source" + "/hdfs-site.xml"));
        //https://blog.csdn.net/qq_26838315/article/details/111941281
        //安全集群和非安全集群之间进行数据迁移时需要配置参数ipc.client.fallback-to-simple-auth-allowed 为 true
        sourceConfig.set("ipc.client.fallback-to-simple-auth-allowed", "true");

        //hfile路径
        String hfileDir = options.get(HFILE_DIR);
        String tmpDir = buildTmpDir(hfileDir, jobInstanceCode);
        String stagingDir = HbaseBulkLoadTool.buildStagingDir(tmpDir);
        logger.info("hfile路径={}", stagingDir);
        LogUtils.info(sparkSession, "hfile路径=" + stagingDir);

        //hfile生成成功后的路径
        String stagingDirSucc = buildStagingDirSucc(stagingDir);
        Path stagingDirPath = new Path(stagingDir);
        Path stagingDirSuccPath = new Path(stagingDirSucc);

        //判断目录是否存在
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

        //生成hfile
        logger.info("开始生成hfile");
        LogUtils.info(sparkSession,"开始生成hfile");
        WriteMode writeMode = EnumUtils.getEnum(WriteMode.class, options.get(WRITE_MODE));
        MappingMode mappingMode = EnumUtils.getEnum(MappingMode.class, options.get(MAPPING_MODE));
        String table = options.get(TABLE);

        Configuration destConfig = new Configuration(false);
        destConfig.addResource(Thread.currentThread().getContextClassLoader().getResource("dest" + "/core-site.xml"));
        destConfig.addResource(Thread.currentThread().getContextClassLoader().getResource("dest" + "/hdfs-site.xml"));
        destConfig.addResource(Thread.currentThread().getContextClassLoader().getResource("dest" + "/hbase-site.xml"));
        Connection destConnection = ConnectionFactory.createConnection(destConfig);

        HbaseTableMeta hbaseTableMeta = null;
        try {
            hbaseTableMeta = HbaseBulkLoadTool.buildHbaseTableMeta(destConnection, table, tmpDir);
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
        int regionSize = hbaseTableMeta.getStartKeys().length;
        logger.info("table={} regionSize={}", table, regionSize);
        LogUtils.info(sparkSession, "table=" + table + " regionSize=" + regionSize);

        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        JavaHBaseContext javaHBaseContext = new JavaHBaseContext(jsc, sourceConfig);

        try {
            if (WriteMode.bulkLoad.equals(writeMode)){
                BulkLoadFunctional functional = null;
                if (MappingMode.one2one.equals(mappingMode)){
                    functional = new One2OneBulkLoadFunction(hbaseTableMeta.getColumnFamily());
                    logger.info("dazhenBulkLoad one2one");
                    LogUtils.info(sparkSession, "dazhenBulkLoad one2one");
                } else {
                    functional = new ArrayZstdBulkLoadFunction(hbaseTableMeta.getColumnFamily(), Bytes.toBytes(options.getOrDefault(MERGE_QUALIFIER, DEFAULT_MERGE_QUALIFIER)));
                    logger.info("dazhenBulkLoad arrayZstd");
                    LogUtils.info(sparkSession, "dazhenBulkLoad arrayZstd");
                }

                HbaseBulkLoadTool.dazhenBulkLoad(javaHBaseContext,
                        dataset,
                        hbaseTableMeta,
                        functional,
                        BooleanUtils.toBoolean(options.get(COMPACTION_EXCLUDE)),
                        Optional.ofNullable(options.get(HFILE_MAX_SIZE)).map((size)->NumberUtils.toLong(size)).orElse(DEFAULT_HFILE_MAX_SIZE),
                        Optional.ofNullable(options.get(HFILE_TIME)).map((time)->NumberUtils.toLong(time)).orElse(System.currentTimeMillis()),
                        options
                        );
                logger.info("writeMode={},mappingMode={} hfile生成成功", writeMode, mappingMode);
                LogUtils.info(sparkSession, "writeMode=" + writeMode + " mappingMode=" + mappingMode + " hfile生成成功");
            } else {
                ThinBulkLoadFunctional functional = null;
                if (MappingMode.one2one.equals(mappingMode)){
                    functional = new One2OneThinBulkLoadFunction(hbaseTableMeta.getColumnFamily());
                    logger.info("dazhenBulkLoadThinRows one2one");
                    LogUtils.info(sparkSession, "dazhenBulkLoadThinRows one2one");
                } else {
                    functional = new ArrayZstdThinBulkLoadFunction(hbaseTableMeta.getColumnFamily(), Bytes.toBytes(options.getOrDefault(MERGE_QUALIFIER, DEFAULT_MERGE_QUALIFIER)));
                    logger.info("dazhenBulkLoadThinRows arrayZstd");
                    LogUtils.info(sparkSession, "dazhenBulkLoadThinRows arrayZstd");
                }
                HbaseBulkLoadTool.dazhenBulkLoadThinRows(javaHBaseContext,
                        dataset,
                        hbaseTableMeta,
                        functional,
                        BooleanUtils.toBoolean(options.get(COMPACTION_EXCLUDE)),
                        Optional.ofNullable(options.get(HFILE_MAX_SIZE)).map((size)->NumberUtils.toLong(size)).orElse(DEFAULT_HFILE_MAX_SIZE),
                        Optional.ofNullable(options.get(HFILE_TIME)).map((time)->NumberUtils.toLong(time)).orElse(System.currentTimeMillis()),
                        options
                );
                logger.info("writeMode={},mappingMode={} hfile生成成功", writeMode, mappingMode);
                LogUtils.info(sparkSession, "writeMode=" + writeMode + " mappingMode=" + mappingMode + " hfile生成成功");
            }
        } catch (Exception e) {
            logger.error("hfile生成失败", e);
            LogUtils.error(sparkSession, "hfile生成失败," + e.getMessage());
            //清理目录
            fileSystem.delete(stagingDirPath, true);
            LogUtils.error(sparkSession, "清理目录" + stagingDirPath);
            throw new DataXException("jobInstanceCode=" + jobInstanceCode + " hfile生成失败", e);
        }

        //hifle目录改成_succ后缀
        if (fileSystem.rename(stagingDirPath, stagingDirSuccPath)){
            logger.info("修改hfile目录名={}成功", stagingDirSuccPath);
            LogUtils.info(sparkSession, "修改hfile目录名=" + stagingDirSuccPath + "成功");
        } else {
            logger.error("修改hfile目录名={}失败", stagingDirSuccPath);
            LogUtils.error(sparkSession, "修改hfile目录名=" + stagingDirSuccPath + "失败");
            throw new DataXException("jobInstanceCode=" + jobInstanceCode + " 修改hfile目录名=" + stagingDirSuccPath + "失败");
        }

        try {
            //统计
            statisticsHfileInfo(sparkSession, sourceConfig, stagingDirSuccPath);
        } catch (Exception e) {
            logger.warn("统计hfile信息异常", e);
            LogUtils.warn(sparkSession, "统计hfile信息异常");
        }

        //是否进行bulkload
        if (BooleanUtils.toBooleanObject(options.get(DO_BULKLOAD))){
            try {
                int maxMaps = Integer.valueOf(options.get(DISTCP_MAXMAPS));
                int mapBandwidth = Integer.valueOf(options.get(DISTCP_MAPBANDWIDTH));

                String distHfileDir = options.get(DISTCP_HFILE_DIR);

                String distTmpDir = null;
                if (StringUtils.isBlank(distHfileDir)){
                    distTmpDir = tmpDir;
                } else {
                    distTmpDir = buildTmpDir(distHfileDir, jobInstanceCode);
                }
                String distStagingDir = HbaseBulkLoadTool.buildStagingDir(distTmpDir);
                logger.info("distCp目录={}", distStagingDir);
                LogUtils.info(sparkSession, "distCp目录=" + distStagingDir);
                UserGroupInformation operateUser = UserGroupInformation.getCurrentUser();
                if (StringUtils.isBlank(destConfig.get("hadoop.security.authentication")) || "simple".equals(destConfig.get("hadoop.security.authentication"))) {
                    operateUser = UserGroupInformation.createRemoteUser("admin");
                }

                FileSystem destFileSystem = operateUser.doAs(new PrivilegedExceptionAction<FileSystem>(){
                    @Override
                    public FileSystem run() throws Exception {
                        return FileSystem.get(destConfig);
                    }
                });

                String nameServices = sourceConfig.get("dfs.nameservices");
                logger.info("nameServices={}", nameServices);
                LogUtils.info(sparkSession,"nameServices=" + nameServices);
                String[] nameServiceArray = nameServices.split(",");

                Path sourcePath = new Path("hdfs://" + nameServiceArray[0] + stagingDirSuccPath);
                Path destPath = new Path("hdfs://" + nameServiceArray[1] + distStagingDir);

                logger.info("sourcePath={},destPath={}", sourcePath, destPath);
                LogUtils.info(sparkSession, "sourcePath=" + sourcePath + ",destPath=" + destPath);

                DistCpUtil.distcp(sourceConfig, Arrays.asList(sourcePath), destPath, maxMaps, mapBandwidth);

                //distcp成功后创建distcp.succ文件
                operateUser.doAs(new PrivilegedExceptionAction<Void>(){
                    @Override
                    public Void run() throws Exception {
                        destFileSystem.create(new Path(distStagingDir, "distcp.succ"));
                        return null;
                    }
                });

                logger.info("distcp成功,开始bulkload");
                LogUtils.info(sparkSession, "distcp成功,开始bulkload");
                String distTempDir = distTmpDir;
                operateUser.doAs(new PrivilegedExceptionAction<Void>(){
                    @Override
                    public Void run() throws Exception {
                        HbaseBulkLoadTool.loadIncrementalHFiles(ConnectionFactory.createConnection(destConfig), table, distTempDir);
                        return null;
                    }
                });

                //删除原集群数据
                fileSystem.delete(stagingDirSuccPath, true);
            } catch (Exception e) {
                logger.error("distcp失败", e);
                LogUtils.error(sparkSession, "distcp失败");
                throw new DataXException("jobInstanceCode=" + jobInstanceCode + " distcp失败", e);

            }
        }
        logger.info("hbaseWriter成功");
        LogUtils.info(sparkSession, "hbaseWriter成功");
    }

    private String buildTmpDir(String stagingDir, String jobInstanceCode){
        StringBuilder sb = new StringBuilder();
        sb.append(LocalDate.now());
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
}
