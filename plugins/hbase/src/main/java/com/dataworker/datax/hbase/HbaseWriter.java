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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.spark.HbaseTableMeta;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.time.LocalDate;
import java.util.*;

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
        Configuration testconfig1 = HBaseConfiguration.create(sparkSession.sparkContext().hadoopConfiguration());
        testconfig1.addResource(Thread.currentThread().getContextClassLoader().getResource("source" + "/hdfs-site.xml"));
        LogUtils.info(sparkSession, "0dfs.nameservices=" + testconfig1.get("dfs.nameservices"));
        LogUtils.info(sparkSession, "0test1test1test1test1=" + testconfig1.get("test1"));
        testconfig1.getFinalParameters().forEach((p)->{
            LogUtils.info(sparkSession, "finalParam=" + p);
            LogUtils.info(sparkSession, "finalValue=" + testconfig1.get(p));
        });


//        sparkSession.sparkContext().hadoopConfiguration().addResource(Thread.currentThread().getContextClassLoader().getResource("source" + "/hdfs-site.xml"));
//        sparkSession.sparkContext().hadoopConfiguration().addResource(Thread.currentThread().getContextClassLoader().getResource("source" + "/mapred-site.xml"));

//        sparkSession.sparkContext()
//                .textFile("hdfs://nameservice1/tmp/hfile/distcp/test2/a.txt",1).collect();
//
//        sparkSession.sparkContext()
//                .textFile("hdfs://nameservice2/tmp/hfile/distcp/test2/a.txt",1).collect();

//        sparkSession.sparkContext()
//                .textFile("hdfs://10.10.9.11:8020/tmp/hfile/distcp/test2/a.txt",1).collect();
//
//        sparkSession.sparkContext().textFile("hdfs://10.10.9.16:8020/tmp/hfile/distcp/test2/a.txt",1).collect();


//        String[] one = (java.lang.String[])sparkSession.sparkContext().textFile("hdfs://10.10.9.11:8020/tmp/hfile/distcp/test2/a.txt",1).collect();
//        String[] two = (java.lang.String[])sparkSession.sparkContext().textFile("hdfs://10.10.9.16:8020/tmp/hfile/distcp/test2/a.txt",1).collect();
//        String[] one = (java.lang.String[])sparkSession.sparkContext().textFile("hdfs://nameservice1/tmp/hfile/distcp/test2/a.txt",1).collect();
//        String[] two = (java.lang.String[])sparkSession.sparkContext().textFile("hdfs://nameservice2/tmp/hfile/distcp/test2/a.txt",1).collect();
//        StringBuilder sbone = new StringBuilder();
//        if (null != one && one.length > 0){
//            for (String str : one){
//                sbone.append(str);
//            }
//        }else{
//            sbone.append("empty");
//        }
//        StringBuilder sbtwo = new StringBuilder();
//        if (null != one && one.length > 0){
//            for (String str : two){
//                sbtwo.append(str);
//            }
//
//        }else {
//            sbtwo.append("empty");
//        }
//
//        LogUtils.info(sparkSession,"sbone=" + sbone.toString());
//        LogUtils.info(sparkSession,"sbtwo=" + sbtwo.toString());

//        try {
//            List<Path> list = new ArrayList<>();
//            list.add(new Path("hdfs://nameservice1/tmp/hfile/distcp/test2/a.txt"));
//
//            DistCpOptions distCpOptions = new DistCpOptions(list, new Path("hdfs://nameservice2/tmp/hfile/distcp/test5/a.txt"));
//            distCpOptions.setMaxMaps(2);
//            distCpOptions.setMapBandwidth(10);
//            distCpOptions.setOverwrite(true);
//            DistCp distcp = new DistCp(sparkSession.sparkContext().hadoopConfiguration(), distCpOptions);
//            distcp.execute();
//        } catch (Exception e) {
//            logger.error("error",e);
//            e.printStackTrace();
//        }



        LogUtils.info(sparkSession, "1dfs.nameservices=" + sparkSession.sparkContext().hadoopConfiguration().get("dfs.nameservices"));
        LogUtils.info(sparkSession,"isSecurityEnabled:" + UserGroupInformation.isSecurityEnabled());
        LogUtils.info(sparkSession,"getCurrentUser():" + UserGroupInformation.getCurrentUser());
        LogUtils.info(sparkSession,"getCurrentUser().getAuthenticationMethod():" + UserGroupInformation.getCurrentUser().getAuthenticationMethod());
        LogUtils.info(sparkSession,"getLoginUser():" + UserGroupInformation.getLoginUser());
        LogUtils.info(sparkSession,"getCurrentUser().getAuthenticationMethod():" + UserGroupInformation.getLoginUser().getAuthenticationMethod());

        LogUtils.info(sparkSession,"hadoopConfiguration=" + sparkSession.sparkContext().hadoopConfiguration() );
        LogUtils.info(sparkSession,"UserGroupInformation.isSecurityEnabled:" + UserGroupInformation.isSecurityEnabled());
        LogUtils.info(sparkSession, "0=" + sparkSession.sparkContext().hadoopConfiguration().get("ipc.client.fallback-to-simple-auth-allowed"));
        //实例编号
        String jobInstanceCode = sparkSession.sparkContext().getConf().get("spark.datawork.job.code");
        if (StringUtils.isBlank(jobInstanceCode)){
            throw new DataXException("实例编号为空");
        }

        if (0 == dataset.count()){
            throw new DataXException("dataset为空");
        }

        LogUtils.info(sparkSession, "1=" + sparkSession.sparkContext().hadoopConfiguration().get("ipc.client.fallback-to-simple-auth-allowed"));
        LogUtils.info(sparkSession, "currentUser=" + UserGroupInformation.getCurrentUser());
        LogUtils.info(sparkSession, "loginUser=" + UserGroupInformation.getLoginUser());
//        Configuration config = HBaseConfiguration.create(sparkSession.sparkContext().hadoopConfiguration());
        Configuration config = new Configuration();
        config.addResource(Thread.currentThread().getContextClassLoader().getResource("source" + "/core-site.xml"));
        config.addResource(Thread.currentThread().getContextClassLoader().getResource("source" + "/yarn-site.xml"));
        config.addResource(Thread.currentThread().getContextClassLoader().getResource("source" + "/hbase-site.xml"));
        config.addResource(Thread.currentThread().getContextClassLoader().getResource("source" + "/mapred-site.xml"));
        config.addResource(Thread.currentThread().getContextClassLoader().getResource("source" + "/hdfs-site.xml"));
        LogUtils.info(sparkSession, "2dfs.nameservices=" + config.get("dfs.nameservices"));
        LogUtils.info(sparkSession, "test1test1test1test1=" + config.get("test1"));
        LogUtils.info(sparkSession, "3=" + config.get("ipc.client.fallback-to-simple-auth-allowed"));
        //https://blog.csdn.net/qq_26838315/article/details/111941281
        //安全集群和非安全集群之间进行数据迁移时需要配置参数ipc.client.fallback-to-simple-auth-allowed 为 true
        config.set("ipc.client.fallback-to-simple-auth-allowed", "true");

        LogUtils.info(sparkSession, "4:" + config.get("ipc.client.fallback-to-simple-auth-allowed"));
        //hfile路径
        String hfileDir = options.get(HFILE_DIR);
        String tmpDir = buildTmpDir(hfileDir, jobInstanceCode);
        String stagingDir = HbaseBulkLoadTool.buildStagingDir(tmpDir);
        logger.info("jobInstanceCode={},hfile路径:{}", jobInstanceCode, stagingDir);
        LogUtils.info(sparkSession, "hfile路径:" + stagingDir);
//        UserGroupInformation.setConfiguration(config);

        //hfile生成成功后的路径
        String stagingDirSucc = buildStagingDirSucc(stagingDir);
        Path stagingDirPath = new Path(stagingDir);
        Path stagingDirSuccPath = new Path(stagingDirSucc);

        //判断目录是否存在
        FileSystem fileSystem = FileSystem.get(config);
        LogUtils.info(sparkSession, "5=" + fileSystem.getConf().get("ipc.client.fallback-to-simple-auth-allowed"));
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
            logger.error("jobInstanceCode={} 清空目录异常", jobInstanceCode, e);
            LogUtils.error(sparkSession, "清空目录异常");
            throw new DataXException("jobInstanceCode=" + jobInstanceCode + "清空目录异常", e);
        }

        //生成hfile
        logger.debug("jobInstanceCode={} 开始生成hfile", jobInstanceCode);
        WriteMode writeMode = EnumUtils.getEnum(WriteMode.class, options.get(WRITE_MODE));
        MappingMode mappingMode = EnumUtils.getEnum(MappingMode.class, options.get(MAPPING_MODE));
        String table = options.get(TABLE);

//        HBaseConfiguration.create()
        Configuration destConfig = new Configuration(false);
        destConfig.addResource(Thread.currentThread().getContextClassLoader().getResource("dest" + "/core-site.xml"));
        destConfig.addResource(Thread.currentThread().getContextClassLoader().getResource("dest" + "/hdfs-site.xml"));
        destConfig.addResource(Thread.currentThread().getContextClassLoader().getResource("dest" + "/hbase-site.xml"));
        Connection destConnection = ConnectionFactory.createConnection(destConfig);

        HbaseTableMeta hbaseTableMeta = null;
        try {
            hbaseTableMeta = HbaseBulkLoadTool.buildHbaseTableMeta(destConnection, table, tmpDir);
            logger.info("jobInstanceCode={}, hbaseTableMeta={}", jobInstanceCode, hbaseTableMeta);
            LogUtils.info(sparkSession, "hbaseTableMeta=" + hbaseTableMeta);
        } catch (Exception e) {
            logger.error("jobInstanceCode={} 创建hbaseTableMeta失败", jobInstanceCode, e);
            LogUtils.error(sparkSession, " 创建hbaseTableMeta失败" + e.getMessage());
            throw new DataXException("jobInstanceCode=" + jobInstanceCode + " 创建hbaseTableMeta失败", e);
        } finally {
            if (!destConnection.isClosed()){
                destConnection.close();
            }
        }
        int regionSize = hbaseTableMeta.getStartKeys().length;
        logger.info("jobInstanceCode={} table={} regionSize={}", jobInstanceCode, table, regionSize);
        LogUtils.info(sparkSession, "table=" + table + " regionSize=" + regionSize);

        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        JavaHBaseContext javaHBaseContext = new JavaHBaseContext(jsc, config);

        try {
            if (WriteMode.bulkLoad.equals(writeMode)){
                BulkLoadFunctional functional = null;
                if (MappingMode.one2one.equals(mappingMode)){
                    functional = new One2OneBulkLoadFunction(hbaseTableMeta.getColumnFamily());
                } else {
                    functional = new ArrayZstdBulkLoadFunction(hbaseTableMeta.getColumnFamily(), Bytes.toBytes(options.getOrDefault(MERGE_QUALIFIER, DEFAULT_MERGE_QUALIFIER)));
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
                logger.info("jobInstanceCode={},writeMode={},mappingMode={} hfile生成成功", jobInstanceCode, writeMode, mappingMode);
                LogUtils.info(sparkSession, "writeMode=" + writeMode + " mappingMode=" + mappingMode + " hfile生成成功");
            } else {
                ThinBulkLoadFunctional functional = null;
                if (MappingMode.one2one.equals(mappingMode)){
                    LogUtils.info(sparkSession, "dazhenBulkLoadThinRows one2one");
                    functional = new One2OneThinBulkLoadFunction(hbaseTableMeta.getColumnFamily());
                } else {
                    functional = new ArrayZstdThinBulkLoadFunction(hbaseTableMeta.getColumnFamily(), Bytes.toBytes(options.getOrDefault(MERGE_QUALIFIER, DEFAULT_MERGE_QUALIFIER)));
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
                logger.info("jobInstanceCode={},writeMode={},mappingMode={} hfile生成成功", jobInstanceCode, writeMode, mappingMode);
                LogUtils.info(sparkSession, "writeMode=" + writeMode + " mappingMode=" + mappingMode + " hfile生成成功");
            }
        } catch (Exception e) {
            logger.error("jobInstanceCode={} hfile生成失败", jobInstanceCode, e);
            LogUtils.error(sparkSession, "jobInstanceCode=" + jobInstanceCode + " hfile生成失败," + e.getMessage());
            //清理目录
            fileSystem.delete(stagingDirPath, true);
            LogUtils.error(sparkSession, "jobInstanceCode=" + jobInstanceCode + "清理目录" + stagingDirPath);
            throw new DataXException("jobInstanceCode=" + jobInstanceCode + " hfile生成失败", e);
        }

        //目录改成_succ后缀
        if (fileSystem.rename(stagingDirPath, stagingDirSuccPath)){
            logger.info("jobInstanceCode={} 修改hfile目录名={}成功", jobInstanceCode, stagingDirSuccPath);
            LogUtils.info(sparkSession, "修改hfile目录名=" + stagingDirSuccPath + "成功");
        } else {
            logger.error("jobInstanceCode={} 修改hfile目录名={}失败", jobInstanceCode, stagingDirSuccPath);
            LogUtils.error(sparkSession, "修改hfile目录名=" + stagingDirSuccPath + "失败");
            throw new DataXException("jobInstanceCode=" + jobInstanceCode + " 修改hfile目录名=" + stagingDirSuccPath + "失败");
        }

        try {
            //统计
            statisticsHfileInfo(sparkSession, config, stagingDirSuccPath, jobInstanceCode);
        } catch (Exception e) {
            logger.warn("jobInstanceCode={} 统计hfile信息异常", jobInstanceCode, e);
            LogUtils.warn(sparkSession, "jobInstanceCode=" + jobInstanceCode + " 统计hfile信息异常");
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
                logger.info("jobInstanceCode={},distCp目录={}", jobInstanceCode, distStagingDir);
                LogUtils.info(sparkSession, "distCp目录=" + distStagingDir);
                //原集群active NN
                InetSocketAddress sourceAddress = HAUtil.getAddressOfActive(fileSystem);
                UserGroupInformation operateUser = UserGroupInformation.getCurrentUser();
                if (StringUtils.isBlank(destConfig.get("hadoop.security.authentication")) || "simple".equals(destConfig.get("hadoop.security.authentication"))) {
                    operateUser = UserGroupInformation.createRemoteUser("admin");
//                    todo
//                    UserGroupInformation.setLoginUser(operateUser);
                }

                LogUtils.info(sparkSession, "currentUser=" + UserGroupInformation.getCurrentUser());
                LogUtils.info(sparkSession, "loginUser=" + UserGroupInformation.getLoginUser());
                LogUtils.info(sparkSession, "operateUser=" + operateUser);

                LogUtils.info(sparkSession, "destConfig.fallback1:" + destConfig.get("ipc.client.fallback-to-simple-auth-allowed"));

                destConfig.set("ipc.client.fallback-to-simple-auth-allowed", "true");
                LogUtils.info(sparkSession, "destConfig.fallback2:" + destConfig.get("ipc.client.fallback-to-simple-auth-allowed"));



//                FileSystem destFileSystem = FileSystem.get(destConfig);
                FileSystem destFileSystem = operateUser.doAs(new PrivilegedExceptionAction<FileSystem>(){
                    @Override
                    public FileSystem run() throws Exception {
                        return FileSystem.get(destConfig);
                    }
                });

                LogUtils.info(sparkSession, "destConfig:" + destConfig);
                LogUtils.info(sparkSession, "destFileSystem:" + destFileSystem);
                LogUtils.info(sparkSession, "fallback:" + destFileSystem.getConf().get("ipc.client.fallback-to-simple-auth-allowed"));
                LogUtils.info(sparkSession, "fs.hdfs.impl.disable.cache:" + destFileSystem.getConf().get("fs.hdfs.impl.disable.cache"));
                LogUtils.info(sparkSession,"UserGroupInformation.isSecurityEnabled:" + UserGroupInformation.isSecurityEnabled());

                //目标集群active NN
//                InetSocketAddress destAddress = HAUtil.getAddressOfActive(destFileSystem);
                InetSocketAddress destAddress = operateUser.doAs(new PrivilegedExceptionAction<InetSocketAddress>(){
                    @Override
                    public InetSocketAddress run() throws Exception {
                        return HAUtil.getAddressOfActive(destFileSystem);
                    }
                });

//                Path sourcePath = new Path("hdfs://" + sourceAddress.getAddress().getHostAddress() + ":" + sourceAddress.getPort() + stagingDirSuccPath);
//                Path destPath = new Path("hdfs://" + destAddress.getAddress().getHostAddress() + ":" + destAddress.getPort() + distStagingDir);

                LogUtils.info(sparkSession, "3dfs.nameservices=" + config.get("dfs.nameservices"));

                Path sourcePath = new Path("hdfs://" + config.get("dfs.nameservices").split(",")[0] + stagingDirSuccPath);
                Path destPath = new Path("hdfs://" + config.get("dfs.nameservices").split(",")[1] + distStagingDir);

                logger.info("sourcePath={},destPath={}", sourcePath, destPath);
                LogUtils.info(sparkSession, "sourcePath=" + sourcePath + ",destPath=" + destPath);

                LogUtils.info(sparkSession,"isSecurityEnabled:" + UserGroupInformation.isSecurityEnabled());
                LogUtils.info(sparkSession,"getCurrentUser():" + UserGroupInformation.getCurrentUser());
                LogUtils.info(sparkSession,"getCurrentUser().getAuthenticationMethod():" + UserGroupInformation.getCurrentUser().getAuthenticationMethod());
                LogUtils.info(sparkSession,"getLoginUser():" + UserGroupInformation.getLoginUser());
                LogUtils.info(sparkSession,"getCurrentUser().getAuthenticationMethod():" + UserGroupInformation.getLoginUser().getAuthenticationMethod());
                LogUtils.info(sparkSession,"operateUser():" + operateUser);
                LogUtils.info(sparkSession,"operateUser().getAuthenticationMethod:" + operateUser.getAuthenticationMethod());


                DistCpUtil.distcp(config,
                        Arrays.asList(sourcePath),
                        destPath,
                        maxMaps,
                        mapBandwidth
                        );

                //distcp成功后创建distcp.succ文件
//                destFileSystem.create(new Path(distStagingDir, "distcp.succ"));
                operateUser.doAs(new PrivilegedExceptionAction<Void>(){
                    @Override
                    public Void run() throws Exception {
                        destFileSystem.create(new Path(distStagingDir, "distcp.succ"));
                        return null;
                    }
                });

                logger.info("jobInstanceCode={} distcp成功", jobInstanceCode);
                LogUtils.info(sparkSession, "distcp成功");
                LogUtils.info(sparkSession,"distTmpDir=" + distTmpDir);

                String distTempDir = distTmpDir;

                operateUser.doAs(new PrivilegedExceptionAction<Void>(){
                    @Override
                    public Void run() throws Exception {
                        HbaseBulkLoadTool.loadIncrementalHFiles(ConnectionFactory.createConnection(destConfig), table, distTempDir);
                        return null;
                    }
                });

//                HbaseBulkLoadTool.loadIncrementalHFiles(ConnectionFactory.createConnection(destConfig), table, distTmpDir);

                //删除原集群数据
                fileSystem.delete(stagingDirSuccPath, true);
            } catch (Exception e) {
                logger.error("jobInstanceCode={} distcp失败", jobInstanceCode, e);
                LogUtils.error(sparkSession, "jobInstanceCode=" + jobInstanceCode + " distcp失败");
                throw new DataXException("jobInstanceCode=" + jobInstanceCode + " distcp失败", e);

            }
        }
        logger.info("jobInstanceCode={} hbaseWriter成功", jobInstanceCode);
        LogUtils.info(sparkSession, "jobInstanceCod=" + jobInstanceCode + " hbaseWriter成功");
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
     * @param jobInstanceCode
     * @throws IOException
     */
    private void statisticsHfileInfo(SparkSession sparkSession, Configuration config, Path path, String jobInstanceCode) throws Exception{
        StringBuilder sb = new StringBuilder(" hfile生成成功, 临时路径：" + path + ":\n");
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
        logger.info("jobInstanceCode={}" + sb.toString(), jobInstanceCode);
        LogUtils.info(sparkSession, sb.toString());
    }
}
