package org.apache.spark.sql.datatunnel.sql

import com.superior.datatunnel.api.{DataTunnelSink, DataTunnelSource, _}
import com.superior.datatunnel.api.model.{
  DataTunnelSinkOption,
  DataTunnelSourceOption
}
import com.superior.datatunnel.common.util.CommonUtils
import com.superior.datatunnel.core.{DataTunnelUtils, Utils}
import io.github.melin.jobserver.spark.api.LogUtils
import io.github.melin.superior.parser.spark.antlr4.SparkSqlParser.DatatunnelExprContext
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.reflect.FieldUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._

/** @author
  *   melin 2021/6/28 2:23 下午
  */
case class DataTunnelExprCommand(sqlText: String, ctx: DatatunnelExprContext)
    extends LeafRunnableCommand
    with Logging {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sourceName = CommonUtils.cleanQuote(ctx.sourceName.getText)
    val sinkName = CommonUtils.cleanQuote(ctx.sinkName.getText)
    val sourceOpts = DataTunnelUtils.convertOptions(ctx.sourceOpts)
    val sinkOpts = DataTunnelUtils.convertOptions(ctx.sinkOpts)
    val transfromSql =
      if (ctx.transfromSql != null)
        CommonUtils.cleanQuote(ctx.transfromSql.getText)
      else null

    val sourceType = DataSourceType.valueOf(sourceName.toUpperCase)
    val sinkType = DataSourceType.valueOf(sinkName.toUpperCase)

    val (sourceConnector, sinkConnector) =
      Utils.getDataTunnelConnector(sourceType, sinkType)
    var errorMsg = s"source $sourceName not have parameter: "
    val sourceOption: DataTunnelSourceOption = CommonUtils.toJavaBean(
      sourceOpts,
      sourceConnector.getOptionClass,
      errorMsg
    )
    sourceOption.setDataSourceType(sourceType)
    if (ctx.ctes() != null) {
      val cteSql = StringUtils.substring(
        sqlText,
        ctx.ctes().start.getStartIndex,
        ctx.ctes().stop.getStopIndex + 1
      )
      sourceOption.setCteSql(cteSql)
    }

    errorMsg = s"sink $sinkName not have parameter: "
    val sinkOption: DataTunnelSinkOption =
      CommonUtils.toJavaBean(sinkOpts, sinkConnector.getOptionClass, errorMsg)
    sinkOption.setDataSourceType(sinkType)

    // 校验 Option
    val sourceViolations = CommonUtils.VALIDATOR.validate(sourceOption)
    if (!sourceViolations.isEmpty) {
      val msg = sourceViolations.asScala
        .map(validator => validator.getMessage)
        .mkString("\n")
      throw new DataTunnelException("Source param is incorrect: \n" + msg)
    }
    val sinkViolations = CommonUtils.VALIDATOR.validate(sinkOption)
    if (!sinkViolations.isEmpty) {
      val msg = sinkViolations.asScala
        .map(validator => validator.getMessage)
        .mkString("\n")
      throw new DataTunnelException("sink param is incorrect: \n" + msg)
    }

    validateOptions(
      sourceName,
      sourceConnector,
      sourceOption,
      sinkName,
      sinkConnector,
      sinkOption
    )

    val context = new DataTunnelContext
    context.setSourceOption(sourceOption)
    context.setSinkOption(sinkOption)
    context.setTransfromSql(transfromSql)
    context.setSourceType(sourceType)
    context.setSinkType(sinkType)

    if (sourceOption.getCteSql != null) {
      if (sourceConnector.supportCte()) {
        val tableName = FieldUtils
          .readField(sourceOption, "tableName", true)
          .asInstanceOf[String]
        val sql = sourceOption.getCteSql + " select * from " + tableName;
        val tdlName = "tdl_datatunnel_cte_" + System.currentTimeMillis
        sparkSession.sql(sql).createTempView(tdlName)
        FieldUtils.writeField(sourceOption, "tableName", tdlName, true)
      } else {
        throw new DataTunnelException(
          "source " + sourceName + " not support cte"
        )
      }
    }
    var df = sourceConnector.read(context)

    val sql = CommonUtils.genOutputSql(
      df,
      sourceOption.getColumns,
      sinkOption.getColumns,
      sinkOption.getDataSourceType
    )
    df = context.getSparkSession.sql(sql)

    if (
      StringUtils.isBlank(sourceOption.getSourceTempView)
      && StringUtils.isNotBlank(transfromSql)
    ) {
      throw new IllegalArgumentException(
        "transfrom 存在，source options 必须指定 sourceTempView"
      )
    } else if (StringUtils.isNotBlank(transfromSql)) {
      df.createTempView(sourceOption.getSourceTempView)
      df = sparkSession.sql(transfromSql)
    }

    sinkConnector.createTable(df, context)
    val schemaInfo = df.schema.treeString(Int.MaxValue)
    LogUtils.info("source schema: \n" + schemaInfo)
    sinkConnector.sink(df, context)
    Seq.empty[Row]
  }

  def validateOptions(
      sourceName: String,
      source: DataTunnelSource,
      sourceOption: DataTunnelSourceOption,
      sinkName: String,
      sink: DataTunnelSink,
      sinkOption: DataTunnelSinkOption
  ): Unit = {
    if (!source.optionalOptions().isEmpty) {
      sourceOption.getProperties.asScala.foreach(item => {
        if (!source.optionalOptions().contains(item._1)) {
          val keys = source
            .optionalOptions()
            .asScala
            .map(key => "properties." + key)
            .mkString(",")
          throw new DataTunnelException(
            s"source $sourceName not have param: properties.${item._1}, Available options: ${keys}"
          )
        }
      })
    }

    if (!sink.optionalOptions().isEmpty) {
      sinkOption.getProperties.asScala.foreach(key => {
        if (!sink.optionalOptions().contains(key._1)) {
          val keys = sink
            .optionalOptions()
            .asScala
            .map(key => "properties." + key)
            .mkString(",")
          throw new DataTunnelException(
            s"sink $sinkName not have param: properties.${key}, Available options: ${keys}"
          )
        }
      })
    }
  }
}
