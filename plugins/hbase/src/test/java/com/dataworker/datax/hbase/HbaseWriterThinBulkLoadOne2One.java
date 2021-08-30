package com.dataworker.datax.hbase;

import com.dataworker.datax.hbase.constant.MappingMode;
import com.dataworker.datax.hbase.constant.WriteMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/****************************************
 * @@CREATE : 2021-08-30 8:20 下午
 * @@AUTH : NOT A CAT【NOTACAT@CAT.ORZ】
 * @@DESCRIPTION :
 * @@VERSION :
 *****************************************/
public class HbaseWriterThinBulkLoadOne2One {
//        create 'bulkLoadTest3', {NAME=>'f1', VERSIONS=>5},SPLITS => ['1', '3', '5', '7']
//        get 'bulkLoadTest3','1',{COLUMN=>'f1',VERSIONS=>5}

    @Test
    public void thinBulkLoadOne2One() throws Exception{
        String table = "bulkLoadTest3";
        String stagingDir = "20210830001";
        String distCpstagingDir = "dist/" + stagingDir;

        String princ = "admin/admin@DZTECH.COM";
        String keytabPath = Thread.currentThread().getContextClassLoader().getResource("source/kerberos-admin.keytab").getPath();
        String krb5Path = Thread.currentThread().getContextClassLoader().getResource("source/krb5.conf").getPath();
        System.setProperty("java.security.krb5.conf", krb5Path);
        System.setProperty("sun.security.krb5.debug", "false");

        Configuration config = new Configuration(false);

        config.addResource(Thread.currentThread().getContextClassLoader().getResource("source/core-site.xml"));
        config.addResource(Thread.currentThread().getContextClassLoader().getResource("source/hbase-site.xml"));
        config.addResource(Thread.currentThread().getContextClassLoader().getResource("source/hdfs-site.xml"));
        config.addResource(Thread.currentThread().getContextClassLoader().getResource("source/yarn-site.xml"));

        UserGroupInformation.setConfiguration(config);
        UserGroupInformation.loginUserFromKeytab(princ, keytabPath);

        Map<String, String> map = new HashMap<>();
        map.put("table", table);
        map.put("writeMode", WriteMode.thinBulkLoad.name());
        map.put("mappingMode", MappingMode.one2one.name());
        map.put("hfileDir", stagingDir);
        map.put("hfileTime", String.valueOf(System.currentTimeMillis()));
        map.put("hfileMaxSize", null);
        map.put("doBulkLoad", "true");
        map.put("mergeQualifier", "merge1");
        map.put("compactionExclude", null);
        map.put("distcp.maxMaps", String.valueOf(2));
        map.put("distcp.mapBandwidth", String.valueOf(100));
        map.put("distcp.hfileDir", distCpstagingDir);

        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("person")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        List<String> list = new ArrayList<String>();
        list.add("zhangsan,1");
        list.add("lisi,2,3");
        list.add("zhaowu,4,5");
        JavaRDD<String> lines = jsc.parallelize(list);

        JavaRDD<Row> rows = lines.map((t)->{
            String[] strArr = t.split(",");
            String name = strArr[0];
            Integer height = Integer.valueOf(strArr[1]);
            Integer weight = null;
            if (strArr.length == 3){
                weight = Integer.valueOf(strArr[2]);
            }
            return RowFactory.create(name, height, weight);
        });

        StructType schema = new StructType(new StructField[] {
                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("height", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("weight", DataTypes.IntegerType, true, Metadata.empty())});
        Dataset<Row> wordDF = sparkSession.createDataFrame(rows, schema);

        HbaseWriter hbaseWriter = new HbaseWriter();

        sparkSession.sparkContext().conf().set("spark.datawork.job.code", "testcode");

        hbaseWriter.write(sparkSession, wordDF, map);

        System.out.println("finish");
    }
}
