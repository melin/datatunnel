### hbaseWriter

#### 说明
* hbase表只能有一个列族,多列族情况不支持
* 不支持DataSet schema只有一列的情况
* 默认取第一列数据为rowkey,第一列数据为空则对当条数据不做处理
* 除了第一列数据外,其他列一一对应
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

| 参数                    | 数据类型    | 是否必填|  默认值        |描述         |
  | :-----               | :-----  | :------| :------         | :------      | 
| table                  | String  | √      |                 |hbase表名  |
| hfileDir               | String  | √      |                 |生成的hfile的路径,后台会转化成 /tmp/hfile/当前时间年月日/hfileDir/实例code  |
| hfileTime              | long    |        |当前时间戳         |生成hfile的时间,默认取当前时间戳 |
| hfileMaxSize           | long    |        |53739520000      |生成hfile的单个最大大小,默认50G |
| compactionExclude      | boolean |        |false            |true\false,storeFile的excludeFromMinorCompaction标志位 |
| doBulkLoad             | boolean | √      |                 |true\false,生成hfile后是否bulkload |
| distcp.hfileDir        | String  |        |hfileDir+"_succ" |distcp 目录，默认和hfileDir+"_succ"保持一致  |
| distcp.maxMaps         | int     | √      |                 |doBulkLoad=true的时候必填,distcp最大map数量 |
| distcp.mapBandwidth    | int     | √      |                 |doBulkLoad=true的时候必填,distcp每个map最大带宽 |
 
 
 
 



 