package com.dataworker.datax.sftp;

import com.dataworker.datax.api.DataxWriter;
import com.dataworker.datax.api.DataXException;
import com.dataworker.datax.sftp.util.SftpUtils;
import com.jcraft.jsch.ChannelSftp;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sparkproject.guava.base.Stopwatch;

import java.io.IOException;
import java.util.Map;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class SftpWriter implements DataxWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SftpWriter.class);

    @Override
    public void validateOptions(Map<String, String> options) {
        String sourceType = options.get("__sourceType__");
        if (!"hdfs".equals(sourceType)) {
            throw new DataXException("只支持从hdfs读取文件写入sftp");
        }

        String remotePath = options.get("path");
        if (StringUtils.isBlank(remotePath)) {
            throw new DataXException("path 不能为空");
        }

        String username = options.get("username");
        if (StringUtils.isBlank(username)) {
            throw new DataXException("username 不能为空");
        }

        String host = options.get("host");
        if (StringUtils.isBlank(host)) {
            throw new DataXException("host 不能为空");
        }
    }

    @Override
    public void write(SparkSession sparkSession, Dataset<Row> dataset, Map<String, String> options) throws IOException {
        ChannelSftp channelSftp = SftpUtils.setupJsch(sparkSession, options);
        try {
            FileSystem fileSystem = FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());
            String remotePath = options.get("path");
            String overwrite = options.getOrDefault("overwrite", "false");
            boolean result = SftpUtils.checkFileExists(channelSftp, remotePath);
            if (!result) {
                SftpUtils.mkdir(channelSftp, remotePath);
            }

            dataset.javaRDD().collect().forEach(row -> {
                try {
                    Path path = new Path(row.getString(0));
                    FSDataInputStream inputStream = fileSystem.open(path);

                    String tmpName = remotePath + "/_" + path.getName() + ".datax";
                    String filename = remotePath + "/" + path.getName();

                    boolean exist = SftpUtils.checkFileExists(channelSftp, filename);
                    if ("false".equals(overwrite) && exist) {
                        LOGGER.warn(path.getName() + " 文件已经存在 不重复上传");
                    } else {
                        Stopwatch stopWatch = new Stopwatch();
                        stopWatch.start();
                        if (exist) {
                            SftpUtils.delete(channelSftp, filename);
                        }
                        exist = SftpUtils.checkFileExists(channelSftp, tmpName);
                        if (exist) {
                            SftpUtils.delete(channelSftp, tmpName);
                        }

                        SftpUtils.upload(channelSftp, inputStream, tmpName);
                        SftpUtils.rename(channelSftp, tmpName, filename);

                        stopWatch.stop();
                        LOGGER.info(path.getName() + " 文件上传成功, 耗时: " + stopWatch.toString());
                    }
                } catch (Exception e) {
                    throw new DataXException("上传文件失败: " + e.getMessage(), e);
                }
            });

            channelSftp.disconnect();
        } finally {
            SftpUtils.close(channelSftp);
        }
    }

}
