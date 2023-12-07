package org.apache.spark.sql.datatunnel.sql

import com.gitee.melin.bee.util.JsonUtils
import com.google.common.collect.Maps
import com.superior.datatunnel.api.DataSourceType._
import com.superior.datatunnel.api.{DataTunnelSink, DataTunnelSource, _}
import com.superior.datatunnel.api.model.{DataTunnelSinkOption, DataTunnelSourceOption}
import com.superior.datatunnel.common.util.CommonUtils
import com.superior.datatunnel.core.Utils
import io.github.melin.superior.parser.spark.antlr4.SparkSqlParser
import io.github.melin.superior.parser.spark.antlr4.SparkSqlParser.DatatunnelExprContext
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.reflect.FieldUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.{Row, SparkSession}

import java.util
import scala.collection.JavaConverters._

/**
 *
 * @author melin 2021/6/28 2:23 下午
 */
case class DataTunnelExprCommand(sqlText: String, ctx: DatatunnelExprContext) extends LeafRunnableCommand with Logging{

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sourceName = CommonUtils.cleanQuote(ctx.sourceName.getText)
    val sinkName = CommonUtils.cleanQuote(ctx.sinkName.getText)
    val sourceOpts = convertOptions(sparkSession, ctx.readOpts)
    val sinkOpts = convertOptions(sparkSession, ctx.writeOpts)
    val transfromSql = if (ctx.transfromSql != null) CommonUtils.cleanQuote(ctx.transfromSql.getText) else null

    val sourceType = DataSourceType.valueOf(sourceName.toUpperCase)
    val sinkType = DataSourceType.valueOf(sinkName.toUpperCase)

    if (KAFKA == sourceType && !(HIVE == sinkType || LOG == sinkType || KAFKA == sinkType
      || DataSourceType.isJdbcDataSource(sinkType))) {
      throw new DataTunnelException("kafka 数据源只能写入 hive hudi表 或者 jdbc 数据源")
    }

    val (sourceConnector, sinkConnector) = Utils.getDatasourceConnector(sourceType, sinkType)
    var errorMsg = s"source $sourceName not have parameter: "
    val sourceOption: DataTunnelSourceOption = CommonUtils.toJavaBean(sourceOpts, sourceConnector.getOptionClass, errorMsg)
    sourceOption.setDataSourceType(sourceType)
    if (ctx.ctes() != null) {
      val cteSql = StringUtils.substring(sqlText, ctx.ctes().start.getStartIndex, ctx.ctes().stop.getStopIndex + 1)
      sourceOption.setCteSql(cteSql)
    }

    errorMsg = s"sink $sinkName not have parameter: "
    val sinkOption: DataTunnelSinkOption = CommonUtils.toJavaBean(sinkOpts, sinkConnector.getOptionClass, errorMsg)
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

    val context = new DataTunnelContext
    context.setSourceOption(sourceOption)
    context.setSinkOption(sinkOption)
    context.setTransfromSql(transfromSql)

    if (sourceOption.getCteSql != null) {
      if (sourceConnector.supportCte()) {
        val tableName = FieldUtils.readField(sourceOption, "tableName", true).asInstanceOf[String]
        val sql = sourceOption.getCteSql + " select * from " + tableName;
        val tdlName = "tdl_datatunnel_cte_" + System.currentTimeMillis
        sparkSession.sql(sql).createTempView(tdlName)
        FieldUtils.writeField(sourceOption, "tableName", tdlName, true)
      } else {
        throw new DataTunnelException("source " + sourceName + " not support cte")
      }
    }
    var df = sourceConnector.read(context)

    val sql = CommonUtils.genOutputSql(df, sourceOption.getColumns, sinkOption.getColumns, sinkOption.getDataSourceType)
    df = context.getSparkSession.sql(sql)

    if (StringUtils.isBlank(sourceOption.getResultTableName)
      && StringUtils.isNotBlank(transfromSql)) {
      throw new IllegalArgumentException("transfrom 存在，source 必须指定 resultTableName")
    } else if (StringUtils.isNotBlank(transfromSql)) {
      if (KAFKA != sourceType) {
        df.createTempView(sourceOption.getResultTableName)
        df = sparkSession.sql(transfromSql)
      }
    }

    if (KAFKA != sourceType) {
      sinkConnector.createTable(df, context)
      sinkConnector.sink(df, context)
    }
    Seq.empty[Row]
  }

  def validateOptions(sourceName: String, source: DataTunnelSource, sourceOption: DataTunnelSourceOption,
                      sinkName: String, sink: DataTunnelSink, sinkOption: DataTunnelSinkOption): Unit = {
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

  def convertOptions(sparkSession: SparkSession, ctx: SparkSqlParser.DtPropertyListContext): util.HashMap[String, String] = {
    val options: util.HashMap[String, String] = Maps.newHashMap()
    if (ctx != null) {
      for (entry <- ctx.dtProperty().asScala) {
        val key = CommonUtils.cleanQuote(entry.key.getText)
        val value = CommonUtils.cleanQuote(entry.value.getText)
        options.put(key, value);
      }
    }

    // superior 通过 datasourceCode 获取数据源链接信息.
    if (options.containsKey("datasourceCode")) {
      val code = options.containsKey("datasourceCode")
      val key = "spark.sql.datatunnel.datasource." + code
      val json = sparkSession.conf.get(key);
      val map = JsonUtils.toJavaMap[String](json);
      options.putAll(map)
    }

    options
  }
}
