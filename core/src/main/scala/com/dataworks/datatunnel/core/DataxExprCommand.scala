package com.dataworks.datatunnel.core

import com.dataworks.datatunnel.api.DataxReader
import com.dataworks.datatunnel.parser.DataxStatementParser.DataxExprContext
import com.dataworks.datatunnel.api.{DataXException, DataxWriter}
import com.dataworks.datatunnel.common.util.CommonUtils
import com.gitee.bee.core.extension.ExtensionLoader
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.{Row, SparkSession}

/**
 *
 * @author melin 2021/6/28 2:23 下午
 */
case class DataxExprCommand(ctx: DataxExprContext) extends LeafRunnableCommand with Logging{

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sourceType = CommonUtils.cleanQuote(ctx.srcName.getText)
    val targetType = CommonUtils.cleanQuote(ctx.distName.getText)
    val readOpts = Utils.convertOptions(ctx.readOpts)
    val writeOpts = Utils.convertOptions(ctx.writeOpts)

    writeOpts.put("__sourceType__", sourceType)
    writeOpts.put("__dataworks__", "no")

    if ("kafka".equals(sourceType) && !"hive".equals(targetType)) {
      throw new DataXException("kafka 数据源只能写入 hive hudi表")
    }

    if ("kafka".equals(sourceType)) {
      readOpts.put("target_databaseName", writeOpts.getOrDefault("databaseName", ""));
      readOpts.put("target_tableName", writeOpts.getOrDefault("tableName", ""));
    }

    val readLoader = ExtensionLoader.getExtensionLoader(classOf[DataxReader])
    val writeLoader = ExtensionLoader.getExtensionLoader(classOf[DataxWriter])

    var reader: DataxReader = null
    var writer: DataxWriter = null
    try {
      reader = readLoader.getExtension(sourceType)
      if (!"kafka".equals(sourceType)) {
        writer = writeLoader.getExtension(targetType)
      }
    } catch {
      case e: IllegalStateException => throw new RuntimeException(e.getMessage, e)
    }

    reader.validateOptions(readOpts)
    if (!"kafka".equals(sourceType)) {
      writer.validateOptions(writeOpts)
    }

    val df = reader.read(sparkSession, readOpts)
    if (!"kafka".equals(sourceType)) {
      writer.write(sparkSession, df, writeOpts)
    }
    Seq.empty[Row]
  }
}
