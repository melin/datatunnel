### HbaseWriter

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

### 参数说明
 
 | 参数                     | 类型    | 必填    |示例值         |
  | :-----                 | :-----  | :------| :------      | 
  | table                  | String  | 是     |hbase表名  |
  | writeMode              | String  | 是     |bulkLoad的模式，支持bulkLoad\thinBulkLoad,一般情况用thinBulkLoad,当列有上万个时候用bulkLoad，对于重复rowKey,两者都存在不确定性 |
  | mappingMode            | String  | 是     |字段映射模式，支持one2one(第一列为rowkey，其他列一一对应)\arrayZstd(第一列为rowkey,其他列合并成一个列) |
  | hfileDir               | String  | 是     |生成的hfile的路径,后台会转化成 /tmp/hfile/当前时间年月日/hfileDir/实例code  |
  | hfileTime              | long    | 否     |生成hfile的时间,默认取当前时间戳 |
  | hfileMaxSize           | long    | 否     |生成hfile的单个最大大小,默认50G |
  | mergeQualifier         | String  | 否     |arrayZstd模式下,合并字段的列名，默认merge  |
  | compactionExclude      | boolean | 否     |true\false，默认false,storeFile的excludeFromMinorCompaction标志位 |
  | doBulkLoad             | boolean | 是     |true\false,生成hfile后是否bulkload |
  | distcp.hfileDir        | String  | 否     |distcp 目录，默认和hfileDir+"_succ"保持一致  |
  | distcp.maxMaps         | int     | 是     |doBulkLoad=true的时候必填,distcp最大map数量 |
  | distcp.mapBandwidth    | int     | 是     |doBulkLoad=true的时候必填,distcp每个map最大带宽 |
 
 
 
 



 