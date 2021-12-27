package com.dataworker.datax.core

import com.dataworker.datax.api.{DataxReader, DataxWriter}
import com.dataworker.datax.core.extension.ExtensionLoader
import com.dataworker.datax.parser.DataxStatementParser.DataxExprContext
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
    val distType = CommonUtils.cleanQuote(ctx.distName.getText)
    val readOpts = CommonUtils.convertOptions(ctx.readOpts)
    val writeOpts = CommonUtils.convertOptions(ctx.writeOpts)

    writeOpts.put("__sourceType__", sourceType)
    writeOpts.put("__dataworks__", "no")

    val readLoader = ExtensionLoader.getExtensionLoader(classOf[DataxReader])
    val writeLoader = ExtensionLoader.getExtensionLoader(classOf[DataxWriter])

    var reader: DataxReader = null
    var writer: DataxWriter = null
    try {
      reader = readLoader.getExtension(sourceType)
      writer = writeLoader.getExtension(distType)
    } catch {
      case e: IllegalStateException => throw new RuntimeException(e.getMessage, e)
    }

    reader.validateOptions(readOpts)
    writer.validateOptions(writeOpts)

    val df = reader.read(sparkSession, readOpts)
    writer.write(sparkSession, df, writeOpts)
    Seq.empty[Row]
  }
}
