### Reader

读取excel 文件

#### 参数说明

| 参数key                         | 数据类型    | 是否必填 | 默认值                 | 描述                                               |
|:------------------------------|:--------|:-----|:--------------------|:-------------------------------------------------|
| path                          | string  |      | 10                  | 文件路径                                             |
| schema                        | string  |      |                     | 自定义excel 字段scheam, 例如: name String, price double |
| dataAddress                   | string  |      | A1                  |                                                  |
| header                        | boolean | √    |                     |                                                  |
| treatEmptyValuesAsNulls       | boolean |      | true                |                                                  |
| setErrorCellsToFallbackValues | boolean |      | false               |                                                  |
| usePlainNumberFormat          | boolean |      | false               |                                                  |
| inferSchema                   | boolean |      | false               |                                                  |
| addColorColumns               | boolean |      | false               |                                                  |
| timestampFormat               | string  |      | yyyy-MM-dd hh:mm:ss |                                                  |
| maxRowsInMemory               | int     |      |                     |                                                  |
| maxByteArraySize              | int     |      |                     |                                                  |
| tempFileThreshold             | int     |      |                     |                                                  |
| excerptSize                   | int     |      | 10                  |                                                  |
| workbookPassword              | string  |      |                     |                                                  |

> 基于spark excel 实现，详细参数请参考：https://github.com/crealytics/spark-excel
