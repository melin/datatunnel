package com.dataworker.datax.hbase.constant;

import com.dazhenyun.hbasesparkproto.function.*;
import org.apache.hadoop.hbase.util.Bytes;

/****************************************
 * @@CREATE : 2021-08-25 7:43 下午
 * @@AUTH : NOT A CAT【NOTACAT@CAT.ORZ】
 * @@DESCRIPTION : 字段映射模式
 * @@VERSION :
 *****************************************/
public enum MappingMode {
    /**
     * 第一列为rowkey,其他列字符串拼接
     */
    stringConcat{
        @Override
        public BulkLoadFunction createBulkLoadFunction(byte[] columnFamily, String mergeQualifier, String separator) {
            return new StringConcatBulkLoadFunction(columnFamily, Bytes.toBytes(mergeQualifier), separator);
        }

        @Override
        public ThinBulkLoadFunction createThinBulkLoadFunction(byte[] columnFamily, String mergeQualifier, String separator) {
            return new StringConcatThinBulkLoadFunction(columnFamily, Bytes.toBytes(mergeQualifier), separator);
        }
    },

    /**
     * 第一列为rowkey,其他列一一映射
     */
    one2one{
        @Override
        public BulkLoadFunction createBulkLoadFunction(byte[] columnFamily, String mergeQualifier, String separator) {
            return new One2OneBulkLoadFunction(columnFamily);
        }

        @Override
        public ThinBulkLoadFunction createThinBulkLoadFunction(byte[] columnFamily, String mergeQualifier, String separator) {
            return new One2OneThinBulkLoadFunction(columnFamily);
        }
    },

    /**
     * 第一列为rowkey,其他列合并成array,然后采用zstd压缩
     */
    arrayZstd{
        @Override
        public BulkLoadFunction createBulkLoadFunction(byte[] columnFamily, String mergeQualifier, String separator) {
            return new ArrayZstdBulkLoadFunction(columnFamily, Bytes.toBytes(mergeQualifier));
        }

        @Override
        public ThinBulkLoadFunction createThinBulkLoadFunction(byte[] columnFamily, String mergeQualifier, String separator) {
            return new ArrayZstdThinBulkLoadFunction(columnFamily, Bytes.toBytes(mergeQualifier));
        }
    };

    public abstract BulkLoadFunction createBulkLoadFunction(byte[] columnFamily, String mergeQualifier, String separator);

    public abstract ThinBulkLoadFunction createThinBulkLoadFunction(byte[] columnFamily, String mergeQualifier, String separator);
}
