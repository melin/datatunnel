package com.dataworks.datatunnel.core

import com.superior.datatunnel.parser.DtunnelStatementParser.DtunnelExprContext
import com.gitee.melin.bee.core.extension.ExtensionLoader
import com.superior.datatunnel.api.{DataTunnelException, DataTunnelSink, DataTunnelSource}
import com.superior.datatunnel.common.util.CommonUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.{Row, SparkSession}

/**
 *
 * @author melin 2021/6/28 2:23 下午
 */
case class DtunnelExprCommand(ctx: DtunnelExprContext) extends LeafRunnableCommand with Logging{

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sourceType = CommonUtils.cleanQuote(ctx.srcName.getText)
    val sinkType = CommonUtils.cleanQuote(ctx.distName.getText)
    val readOpts = Utils.convertOptions(ctx.readOpts)
    val writeOpts = Utils.convertOptions(ctx.writeOpts)

    readOpts.put("__sinkType__", sinkType)
    writeOpts.put("__sourceType__", sourceType)

    if ("kafka".equals(sourceType) && !("hive".equals(sinkType) || "jdbc".equals(sinkType))) {
      throw new DataTunnelException("kafka 数据源只能写入 hive hudi表 或者 jdbc 数据源")
    }

    if ("kafka".equals(sourceType)) {
      writeOpts.forEach((key, value) => {
        readOpts.put("_sink_" + key, value);
      })
    }

    val readLoader = ExtensionLoader.getExtensionLoader(classOf[DataTunnelSource])
    val writeLoader = ExtensionLoader.getExtensionLoader(classOf[DataTunnelSink])

    var source: DataTunnelSource = null
    var sink: DataTunnelSink = null
    try {
      source = readLoader.getExtension(sourceType)
      if (!"kafka".equals(sourceType)) {
        sink = writeLoader.getExtension(sinkType)
      }
    } catch {
      case e: IllegalStateException => throw new RuntimeException(e.getMessage, e)
    }

    source.validateOptions(readOpts)
    if (!"kafka".equals(sourceType)) {
      sink.validateOptions(writeOpts)
    }

    val df = source.read(sparkSession, readOpts)
    if (!"kafka".equals(sourceType)) {
      sink.write(sparkSession, df, writeOpts)
    }
    Seq.empty[Row]
  }
}
