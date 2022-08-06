### Reader

读取excel 文件

#### 参数说明

| 参数key                         | 数据类型    | 是否必填 | 默认值                             | 描述   |
|:------------------------------|:--------|:-----|:--------------------------------|:-----|
| file                          | string  |      | 10                              | 文件路径 |
| dataAddress                   | string  |      | A1                                |      |
| header                        | boolean | √    |                                 |      |
| treatEmptyValuesAsNulls       | boolean |      | true                            |      |
| setErrorCellsToFallbackValues | boolean |      | false                           |      |
| usePlainNumberFormat          | boolean |      | false                           |      |
| inferSchema                   | boolean |      | false                           |      |
| addColorColumns               | boolean |      | false                           |      |
| timestampFormat               | string  |      | yyyy-mm-dd hh:mm:ss[.fffffffff] |      |
| maxRowsInMemory               | int     |      |                                 |      |
| maxByteArraySize              | int     |      |                                 |      |
| excerptSize                   | int     |      | 10                              |      |
| workbookPassword              | string  |      |                                 |      |

> 基于spark excel 实现，详细参数请参考：https://github.com/crealytics/spark-excel
