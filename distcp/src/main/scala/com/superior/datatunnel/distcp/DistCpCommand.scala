package com.superior.datatunnel.distcp

import com.google.common.collect.Maps
import com.superior.datatunnel.common.util.CommonUtils
import io.github.melin.superior.parser.spark.antlr4.SparkSqlParser
import io.github.melin.superior.parser.spark.antlr4.SparkSqlParser.DistCpExprContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.LeafRunnableCommand

import java.util
import scala.collection.JavaConverters._

case class DistCpCommand(sqlText: String, ctx: DistCpExprContext) extends LeafRunnableCommand with Logging{

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sourcePath = CommonUtils.cleanQuote(ctx.sourcePath.getText)
    val sinkPath = CommonUtils.cleanQuote(ctx.sinkPath.getText)
    val sourceOpts = convertOptions(ctx.sourceOpts)
    val sinkOpts = convertOptions(ctx.sinkOpts)

    Seq.empty[Row]
  }

  def convertOptions(ctx: SparkSqlParser.DtPropertyListContext): util.HashMap[String, String] = {
    val options: util.HashMap[String, String] = Maps.newHashMap()
    if (ctx != null) {
      for (entry <- ctx.dtProperty().asScala) {
        val key = CommonUtils.cleanQuote(entry.key.getText)
        val value = CommonUtils.cleanQuote(entry.value.getText)
        options.put(key, value);
      }
    }
    options
  }
}
