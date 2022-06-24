package com.dataworks.datatunnel.core

import com.superior.datatunnel.parser.DtunnelStatementParser.DtunnelExprContext
import com.gitee.melin.bee.core.extension.ExtensionLoader
import com.superior.datatunnel.api.model.{SinkOption, SourceOption}
import com.superior.datatunnel.api.{DataSourceType, DataTunnelContext, DataTunnelException, DataTunnelSink, DataTunnelSource}
import com.superior.datatunnel.common.util.CommonUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.{Row, SparkSession}

/**
 *
 * @author melin 2021/6/28 2:23 下午
 */
case class DataTunnelExprCommand(ctx: DtunnelExprContext) extends LeafRunnableCommand with Logging{

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sourceName = CommonUtils.cleanQuote(ctx.sourceName.getText)
    val sinkName = CommonUtils.cleanQuote(ctx.sinkName.getText)
    val sourceOpts = Utils.convertOptions(ctx.sourceOpts)
    val sinkOpts = Utils.convertOptions(ctx.sinkOpts)

    val sourceType = DataSourceType.valueOf(sourceName.toUpperCase)
    val sinkType = DataSourceType.valueOf(sinkName.toUpperCase)

    if ("kafka".equals(sourceType) && !("hive".equals(sinkType) || "jdbc".equals(sinkType))) {
      throw new DataTunnelException("kafka 数据源只能写入 hive hudi表 或者 jdbc 数据源")
    }

    val readLoader = ExtensionLoader.getExtensionLoader(classOf[DataTunnelSource])
    val writeLoader = ExtensionLoader.getExtensionLoader(classOf[DataTunnelSink])

    var source: DataTunnelSource = null
    var sink: DataTunnelSink = null
    try {
      source = readLoader.getExtension(sourceName)
      if (!"kafka".equals(sourceType)) {
        sink = writeLoader.getExtension(sinkName)
      }
    } catch {
      case e: IllegalStateException => throw new RuntimeException(e.getMessage, e)
    }

    // @TODO
    val sourceOption: SourceOption = null
    val sinkOption: SinkOption = null

    val context = new DataTunnelContext
    context.setSourceOption(sourceOption)
    context.setSinkOption(sinkOption)

    val df = source.read(context)
    if (!"kafka".equals(sourceType)) {
      sink.sink(df, context)
    }
    Seq.empty[Row]
  }
}
