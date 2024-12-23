package org.apache.spark.sql.datatunnel.sql

import com.superior.datatunnel.api.model.DistCpOption
import com.superior.datatunnel.api.{DataTunnelException, DistCpContext}
import com.superior.datatunnel.common.util.{CommonUtils, DatatunnelUtils}
import com.superior.datatunnel.core.DataTunnelUtils
import io.github.melin.superior.parser.spark.antlr4.SparkSqlParser.DistCpExprContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._

case class DistCpCommand(sqlText: String, ctx: DistCpExprContext) extends LeafRunnableCommand with Logging {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sourceOpts = DataTunnelUtils.convertOptions(ctx.options)

    val distCpAction = DatatunnelUtils.getDistCpAction()
    val errorMsg = s"distcp option not have parameter: "
    val option: DistCpOption =
      CommonUtils.toJavaBean(sourceOpts, classOf[DistCpOption], errorMsg)

    // 校验 Option
    val optionViolations = CommonUtils.VALIDATOR.validate(option)
    if (!optionViolations.isEmpty) {
      val msg = optionViolations.asScala
        .map(validator => validator.getMessage)
        .mkString("\n")
      throw new DataTunnelException("distcp param is incorrect: \n" + msg)
    }

    val context = new DistCpContext
    context.setOption(option)
    distCpAction.run(context)
    Seq.empty[Row]
  }
}
