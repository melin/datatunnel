package com.dataworker.datax.hbase.constant;

/****************************************
 * @@CREATE : 2021-08-25 7:43 下午
 * @@AUTH : NOT A CAT【NOTACAT@CAT.ORZ】
 * @@DESCRIPTION : 字段映射模式
 * @@VERSION :
 *****************************************/
public enum MappingMode {
    /**
     * 第一列为rowkey,其他列一一映射
     */
    one2one,

    /**
     * 第一列为rowkey,其他列合并成array,然后采用zstd压缩
     */
    arrayZstd;

}
