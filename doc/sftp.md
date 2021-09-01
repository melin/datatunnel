### Reader 参数

| 参数key            | 是否必填  | 默认值    |描述                                  |
| :-----            | :-----   | :------  | :------                             |
| path              | √        |          |            |
| username          | √        |          |            |
| password          |          |          |            |
| host              | √        |          |            |
| port              | √        | 22       |            |
| fileType          | √        | 22       |Supported types are csv, txt, json, avro and parquet            |

sftp 基于 https://github.com/springml/spark-sftp 实现，更多参数请参考spark-sftp文档


### Writer 参数

| 参数key            | 是否必填  | 默认值    |描述                                  |
| :-----            | :-----   | :------  | :------                             |
| path              | √        |          |            |
| username          | √        |          |            |
| password          |          |          |如果指定 keyFilePath 值，优先使用秘钥认证          |
| host              | √        |          |            |
| port              | √        | 22       |            |
| keyFilePath       |          |          |秘钥文件路径，用户上传存在[我的资源]中          |
| passPhrase        |          |          |密钥的密码         |
| overwrite         |          | false    |相同文件已经存在，是否覆盖写入。等于true，第一此执行失败，后续重复执行，实现续传的效果|
