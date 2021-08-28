package com.dataworker.datax.hbase.constant;

/****************************************
 * @@CREATE : 2021-08-25 7:41 下午
 * @@AUTH : NOT A CAT【NOTACAT@CAT.ORZ】
 * @@DESCRIPTION : 一般情况用thinBulkLoad,当列有上万个时候用bulkLoad，对于重复rowKey,两者都存在不确定性
 * @@VERSION :
 *****************************************/
public enum WriteMode {

    bulkLoad,

    thinBulkLoad
}
