
### Reader

#### 参数说明
| 参数key            | 数据类型  | 是否必填  | 默认值    |描述                                  |
| :-----            | :-----   | :-----   | :------  | :------                             |
| path              | string   | √        |          |            |
| username          | string   | √        |          |            |
| password          | string   |          |          |            |
| host              | string   | √        |          |            |
| port              | int      | √        | 22       |            |
| fileType          | string   | √        | 22       |Supported types are csv, txt, json, avro and parquet            |


### Writer

#### 参数说明

| 参数key            | 数据类型  | 是否必填  | 默认值    |描述                                  |
| :-----            | :-----   | :-----   | :------  | :------                             |
| path              | string   | √        |          |            |
| username          | string   | √        |          |            |
| password          | string   |          |          |如果指定 keyFilePath 值，优先使用秘钥认证          |
| host              | string   | √        |          |            |
| port              | int      | √        | 22       |            |
| keyFilePath       | string   |          |          |秘钥文件路径，用户上传存在[我的资源]中          |
| passPhrase        | string   |          |          |密钥的密码         |
| overwrite         | boolean  |          | false    |相同文件已经存在，是否覆盖写入。等于true，第一此执行失败，后续重复执行，实现续传的效果|
