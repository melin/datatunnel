package com.superior.datatunnel.plugin.hdfs;

import com.superior.datatunnel.api.DataTunnelContext;
import com.superior.datatunnel.api.DataTunnelSource;
import com.superior.datatunnel.api.DataTunnelException;
import com.superior.datatunnel.api.model.DataTunnelSourceOption;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class HdfsDataTunnelSource implements DataTunnelSource {

    @Override
    public Dataset<Row> read(DataTunnelContext context) throws IOException {
        String userId = context.getSparkSession().conf().get("spark.datawork.job.userId", "");
        HdfsDataTunnelSourceOption sourceOption = (HdfsDataTunnelSourceOption) context.getSourceOption();

        if (StringUtils.isBlank(userId)) {
            throw new DataTunnelException("spark.superior.job.userId 不能为空");
        }

        String path = sourceOption.getPath();
        String pathPrefix = "/user/superior/users/" + userId;
        if (!StringUtils.startsWith(path, pathPrefix)) {
            throw new DataTunnelException("只能访问 " + pathPrefix + " 路径下文件");
        }

        String fileNameSuffix = sourceOption.getFileNameSuffix();

        Configuration configuration = context.getSparkSession().sparkContext().hadoopConfiguration();
        Stream<String> stream = Arrays.stream(FileSystem.get(configuration).listStatus(new Path(path)))
                .map(file -> file.getPath().toString());

        if (StringUtils.isNotBlank(fileNameSuffix)) {
            stream = stream.filter(file -> StringUtils.endsWith(file, fileNameSuffix));
        }

        List<String> paths = stream.collect(Collectors.toList());
        return context.getSparkSession().sqlContext().createDataset(paths, Encoders.STRING()).toDF();
    }

    @Override
    public Class<? extends DataTunnelSourceOption> getOptionClass() {
        return HdfsDataTunnelSourceOption.class;
    }
}
