### hbaseWriter

#### 说明
* hbase表只能有一个列族,多列族情况不支持
* 不支持DataSet schema只有一列的情况
* 默认取Reader得到的第一列数据为rowkey,第一列数据为空则对当条数据不做处理
* 除了rowkey,剩余的数据按照英文逗号,拼接
* 对于rowKey重复的情况下,hbasewriter随机保存一条
* 生成的hfile文件目录规则为/tmp/hfile/{当前时间年月日}/{hfileDir}/{实例code},生成成功后会加上_succ
* 默认情况下distcp后的hfile文件目录和生成hfile文件目录一致,distcp成功后会在目录下生成distcp.succ文件
* 建议按照数据量和数据结构提前对hbase表建立好region划分,有助于提高hfile生成速度

#### 支持的数据类型
| 类型       | 说明    | 
| :-----    | :-----  | 
| String    |   |
| Integer   |   |
| Long      |   |
| Double    |   |
| Short     |   |
| Float     |   |
| byte[]    |   |
| boolean   |   |
| Date      |最后以long存入hbase |
| TimeStamp |最后以long存入hbase |

#### 参数说明

| 参数                       | 数据类型    | 是否必填|  默认值        |描述         |
  | :-----                  | :-----  | :------| :------         | :------      | 
| tableName                 | String  | √      |                 |hbase表名  |
| hfileDir                  | String  | √      |                 |生成的hfile的路径,后台会转化成 /tmp/hfile/当前时间年月日/hfileDir/实例code  |
| mergeQualifier            | String  |        |merge            |列名 |
| separator                 | String  |        |,                |字符串拼接的分隔符 |
| hfileTime                 | long    |        |当前时间戳         |生成hfile的时间,默认取当前时间戳 |
| hfileMaxSize              | long    |        |10747904000      |生成hfile的单个最大大小,默认10G |
| distcp.hfileDir           | String  |        |{hfileDir}       |distcp 目录，默认和hfileDir保持一致  |
| distcp.maxMaps            | int     |        |5                |doBulkLoad=true的时候必填,distcp最大map数量 |
| distcp.perMapBandwidth    | int     |        |100              |doBulkLoad=true的时候必填,distcp每个map最大带宽 |
 
#### 示例

```
writer("hbase") options(tableName="bulkLoadTest1", hfileDir='test2')
```

```
writer("hbase") options(tableName="bulkLoadTest1", hfileDir='test2',mergeQualifier='m',separator=',',hfileTime='1636448869098',hfileMaxSize=10747904000,distcp.hfileDir='dist2/test2',distcp.maxMaps=5,distcp.perMapBandwidth=100)
```



 