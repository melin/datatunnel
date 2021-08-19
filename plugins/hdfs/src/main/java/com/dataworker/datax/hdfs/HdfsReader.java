package com.dataworker.datax.hdfs;

import com.dataworker.datax.api.DataxReader;
import com.dataworker.datax.api.DataXException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author melin 2021/7/27 11:06 上午
 */
public class HdfsReader implements DataxReader {

    @Override
    public void validateOptions(Map<String, String> options) {
        if (!options.containsKey("path")) {
            throw new DataXException("缺少path 参数");
        } else {
            String path = options.get("path");
            if (StringUtils.isBlank(path)) {
                throw new DataXException("path 不能为空");
            }
        }
    }

    @Override
    public Dataset<Row> read(SparkSession sparkSession, Map<String, String> options) throws IOException {
        String userId = sparkSession.conf().get("spark.datawork.job.userId", "");
        if (StringUtils.isBlank(userId)) {
            throw new DataXException("spark.datawork.job.userId 不能为空");
        }

        String path = options.get("path");
        String pathPrefix = "/user/datawork/users/" + userId;
        if (!StringUtils.startsWith(path, pathPrefix)) {
            throw new DataXException("只能访问 " + pathPrefix + " 路径下文件");
        }

        String fileNameSuffix = options.get("fileNameSuffix");

        Configuration configuration = sparkSession.sparkContext().hadoopConfiguration();
        Stream<String> stream = Arrays.stream(FileSystem.get(configuration).listStatus(new Path(path)))
                .map(file -> file.getPath().toString());

        if (StringUtils.isNotBlank(fileNameSuffix)) {
            stream = stream.filter(file -> StringUtils.endsWith(file, fileNameSuffix));
        }

        List<String> paths = stream.collect(Collectors.toList());
        return sparkSession.sqlContext().createDataset(paths, Encoders.STRING()).toDF();
    }
}
