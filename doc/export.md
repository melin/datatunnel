### 导出数据

```
-- 通过文件名后缀，指定导出文件格式，目前支持：txt、csv、json、excel 三种文件
WITH common_table_expression [ , ... ]
export table tablename [PARTITION (part_column="value"[, ...])] TO 'export_file_name.[txt|csv|json|xlsx]' [options(key=value)]
```

### 支持CTE
通过CTE 加工数据以后，导出数据：
```sql
-- 导出指定分区数据
with
tdl_test_with as (select * from test_users_dt where ds ='20171011')
export table tdl_test_with TO 'tdl_test_with' options (compression=true)

-- 从mysql 导出数据
CREATE TEMPORARY VIEW mysql_demos
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "jdbc:mysql://172.18.5.44:3306/demos",
  dbtable "users",
  user 'root',
  password 'root2023'
)
export table mysql_demos TO '/Users/melin/Documents/users.csv' options(delimiter=';')
```

### 参数
fileCount: csv和json 支持设置导出文件数量: `dataFrame.coalesce(fileCount)`, excel始终只有一个文件，所以太大数据不要使用excel文件格式: 

### 导出 CSV 参数

csv 相关参数，请参考spark csv 文档: https://spark.apache.org/docs/latest/sql-data-sources-csv.html

```
-- delimiter表示csv文件分隔符
export table raw_activity_flat PARTITION (year=2018, month=3, day=12) TO 'activity_20180312.csv' options(delimiter=';')
```

```
-- 覆盖原文件导出, 添加参数：overwrite = true
export table raw_activity_flat PARTITION (year=2018, month=3, day=12) TO 'activity_20180312.csv' options(delimiter=';',overwrite=true)
```

```
-- 默认不支持array,map,struct 复杂类型的数据导出，添加参数：complexTypeToJson = true，复杂类型数据转为json string 导出
export table raw_activity_flat PARTITION (year=2018, month=3, day=12) TO 'activity_20180312.csv' options(delimiter=';',complexTypeToJson = true)
```

### 导出 JSON 参数

JSON 相关参数，请参考 spark json 文档: https://spark.apache.org/docs/latest/sql-data-sources-json.html

### 导出 Text 参数

Text 相关参数，请参考 spark text 文档: https://spark.apache.org/docs/latest/sql-data-sources-text.html

### 导出 Excel 参数

Excel 相关参数，请参考 spark excel 文档: https://github.com/crealytics/spark-excel

