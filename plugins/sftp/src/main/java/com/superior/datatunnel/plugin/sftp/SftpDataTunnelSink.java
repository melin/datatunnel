package com.superior.datatunnel.plugin.sftp;

import com.superior.datatunnel.api.*;
import com.superior.datatunnel.api.model.DataTunnelSinkOption;
import com.superior.datatunnel.plugin.sftp.util.SftpUtils;
import com.github.melin.superior.jobserver.api.LogUtils;
import com.jcraft.jsch.ChannelSftp;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sparkproject.guava.base.Stopwatch;

import java.io.IOException;

import static com.superior.datatunnel.api.DataSourceType.HDFS;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class SftpDataTunnelSink implements DataTunnelSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(SftpDataTunnelSink.class);

    private void validateOptions(DataTunnelContext context) {
        DataSourceType dsType = context.getSourceOption().getDataSourceType();
        if (HDFS == dsType) {
            throw new DataTunnelException("只支持从hdfs读取文件写入sftp");
        }
    }

    @Override
    public void sink(Dataset<Row> dataset, DataTunnelContext context) throws IOException {
        validateOptions(context);

        ChannelSftp channelSftp = SftpUtils.setupJsch(context.getSparkSession(), context.getSinkOption().getParams());
        SftpDataTunnelSinkOption sinkOption = (SftpDataTunnelSinkOption) context.getSinkOption();
        try {
            FileSystem fileSystem = FileSystem.get(context.getSparkSession().sparkContext().hadoopConfiguration());
            String remotePath = sinkOption.getPath();
            boolean overwrite = sinkOption.isOverwrite();
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
                    if (!overwrite && exist) {
                        LogUtils.error(path.getName() + " 文件已经存在 不重复上传");
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
                        LogUtils.info(path.getName() + " 文件上传成功, 耗时: " + stopWatch);
                    }
                } catch (Exception e) {
                    throw new DataTunnelException("上传文件失败: " + e.getMessage(), e);
                }
            });

            channelSftp.disconnect();
        } finally {
            SftpUtils.close(channelSftp);
        }
    }

    @Override
    public Class<? extends DataTunnelSinkOption> getOptionClass() {
        return SftpDataTunnelSinkOption.class;
    }
}
