package org.apache.spark.sql.datatunnel.sql

import com.superior.datatunnel.api.{DataSourceType, DataTunnelException, DistCpContext, DistCpSink, DistCpSource}
import com.superior.datatunnel.api.model.{DistCpBaseSinkOption, DistCpBaseSourceOption}
import com.superior.datatunnel.common.util.CommonUtils
import com.superior.datatunnel.core.{DataTunnelUtils, Utils}
import io.github.melin.superior.parser.spark.antlr4.SparkSqlParser.DistCpExprContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._

case class DistCpCommand(sqlText: String, ctx: DistCpExprContext) extends LeafRunnableCommand with Logging{

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sourceName = CommonUtils.cleanQuote(ctx.sourceName.getText)
    val sinkName = CommonUtils.cleanQuote(ctx.sinkName.getText)
    val sourceOpts = DataTunnelUtils.convertOptions(ctx.sourceOpts)
    val sinkOpts = DataTunnelUtils.convertOptions(ctx.sinkOpts)

    val sourceType = DataSourceType.valueOf(sourceName.toUpperCase)
    val sinkType = DataSourceType.valueOf(sinkName.toUpperCase)

    val (sourceConnector, sinkConnector) = Utils.getDistCpConnector(sourceType, sinkType)
    var errorMsg = s"source $sourceName not have parameter: "
    val sourceOption: DistCpBaseSourceOption = CommonUtils.toJavaBean(sourceOpts, sourceConnector.getOptionClass, errorMsg)
    sourceOption.setDataSourceType(sourceType)

    errorMsg = s"sink $sinkName not have parameter: "
    val sinkOption: DistCpBaseSinkOption = CommonUtils.toJavaBean(sinkOpts, sinkConnector.getOptionClass, errorMsg)
    sinkOption.setDataSourceType(sinkType)

    // 校验 Option
    val sourceViolations = CommonUtils.VALIDATOR.validate(sourceOption)
    if (!sourceViolations.isEmpty) {
      val msg = sourceViolations.asScala.map(validator => validator.getMessage).mkString("\n")
      throw new DataTunnelException("Source param is incorrect: \n" + msg)
    }
    val sinkViolations = CommonUtils.VALIDATOR.validate(sinkOption)
    if (!sinkViolations.isEmpty) {
      val msg = sinkViolations.asScala.map(validator => validator.getMessage).mkString("\n")
      throw new DataTunnelException("sink param is incorrect: \n" + msg)
    }

    validateOptions(sourceName, sourceConnector, sourceOption, sinkName, sinkConnector, sinkOption)

    val context = new DistCpContext
    context.setSourceOption(sourceOption)
    context.setSinkOption(sinkOption)

    val df = sourceConnector.read(context)
    sinkConnector.sink(df, context)
    Seq.empty[Row]
  }

  def validateOptions(sourceName: String, source: DistCpSource, sourceOption: DistCpBaseSourceOption,
                      sinkName: String, sink: DistCpSink, sinkOption: DistCpBaseSinkOption): Unit = {
    if (!source.optionalOptions().isEmpty) {
      sourceOption.getProperties.asScala.foreach(item => {
        if (!source.optionalOptions().contains(item._1)) {
          val keys = source.optionalOptions().asScala.map(key => "properties." + key).mkString(",")
          throw new DataTunnelException(s"source $sourceName not have param: properties.${item._1}, Available options: ${keys}")
        }
      })
    }

    if (!sink.optionalOptions().isEmpty) {
      sinkOption.getProperties.asScala.foreach(key => {
        if (!sink.optionalOptions().contains(key)) {
          val keys = sink.optionalOptions().asScala.map(key => "properties." + key).mkString(",")
          throw new DataTunnelException(s"sink $sinkName not have param: properties.${key}, Available options: ${keys}")
        }
      })
    }
  }
}
