package com.dataworks.datatunnel.core

import com.superior.datatunnel.parser.DtunnelStatementParser.DtunnelExprContext
import com.gitee.melin.bee.core.extension.ExtensionLoader
import com.superior.datatunnel.api.model.{DataTunnelSinkOption, DataTunnelSourceOption}
import com.superior.datatunnel.api._
import com.superior.datatunnel.common.util.CommonUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.{Row, SparkSession}
import com.superior.datatunnel.api.DataSourceType._
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConverters._

import javax.validation.{Validation, Validator, ValidatorFactory}

/**
 *
 * @author melin 2021/6/28 2:23 下午
 */
case class DataTunnelExprCommand(ctx: DtunnelExprContext) extends LeafRunnableCommand with Logging{

  val factory: ValidatorFactory = Validation.buildDefaultValidatorFactory

  val validator: Validator = factory.getValidator

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sourceName = CommonUtils.cleanQuote(ctx.sourceName.getText)
    val sinkName = CommonUtils.cleanQuote(ctx.sinkName.getText)
    val sourceOpts = Utils.convertOptions(ctx.sourceOpts)
    val sinkOpts = Utils.convertOptions(ctx.sinkOpts)

    val transfromSql = if (ctx.transfromSql != null) CommonUtils.cleanQuote(ctx.transfromSql.getText) else null

    val sourceType = DataSourceType.valueOf(sourceName.toUpperCase)
    val sinkType = DataSourceType.valueOf(sinkName.toUpperCase)

    if (KAFKA == sourceType && !(HIVE == sinkType || DataSourceType.isJdbcDataSource(sinkType))) {
      throw new DataTunnelException("kafka 数据源只能写入 hive hudi表 或者 jdbc 数据源")
    }

    val readLoader = ExtensionLoader.getExtensionLoader(classOf[DataTunnelSource])
    val writeLoader = ExtensionLoader.getExtensionLoader(classOf[DataTunnelSink])

    var source: DataTunnelSource = null;
    var sink: DataTunnelSink = null
    try {
      source = readLoader.getExtension(sourceName)
      if (KAFKA != sourceType) {
        sink = writeLoader.getExtension(sinkName)
      }
    } catch {
      case e: IllegalStateException => throw new RuntimeException(e.getMessage, e)
    }

    val sourceOption: DataTunnelSourceOption = CommonUtils.toJavaBean(sourceOpts, source.getOptionClass)
    sourceOption.setDataSourceType(sourceType)
    val sinkOption: DataTunnelSinkOption = CommonUtils.toJavaBean(sinkOpts, sink.getOptionClass)
    sinkOption.setDataSourceType(sinkType)

    val sourceViolations = validator.validate(sourceOption)
    if (!sourceViolations.isEmpty) {
      val msg = sourceViolations.asScala.map(validator => validator.getMessage).mkString("\n")
      throw new DataTunnelException("Source param is incorrect: \n" + msg)
    }
    val sinkViolations = validator.validate(sinkOption)
    if (!sinkViolations.isEmpty) {
      val msg = sinkViolations.asScala.map(validator => validator.getMessage).mkString("\n")
      throw new DataTunnelException("sink param is incorrect: \n" + msg)
    }

    val context = new DataTunnelContext
    context.setSourceOption(sourceOption)
    context.setSinkOption(sinkOption)

    var df = source.read(context)

    if (StringUtils.isBlank(sourceOption.getResultTableName)
      && StringUtils.isNotBlank(transfromSql)) {
      throw new IllegalArgumentException("transfrom 存在，source 必须指定 resultTableName")
    } else if (StringUtils.isNotBlank(transfromSql)) {
      df.createTempView(sourceOption.getResultTableName)
      df = SparkSession.active.sql(transfromSql)
    }

    if (KAFKA != sourceType) {
      sink.sink(df, context)
    }
    Seq.empty[Row]
  }
}
