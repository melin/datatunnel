package org.apache.spark.sql.datatunnel.sql

import com.google.common.collect.Lists
import com.superior.datatunnel.api.DataSourceType
import com.superior.datatunnel.common.util.CommonUtils
import com.superior.datatunnel.core.{DataTunnelUtils, Utils}
import io.github.melin.superior.parser.spark.antlr4.SparkSqlParser.DatatunnelHelpContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import java.util

/** @author
  *   melin 2021/6/28 2:23 下午
  */
case class DataTunnelHelpCommand(sqlText: String, ctx: DatatunnelHelpContext)
    extends LeafRunnableCommand
    with Logging {

  private val OUTPUT_TYPE = new StructType(
    Array[StructField](
      StructField(
        "type",
        DataTypes.StringType,
        nullable = true,
        Metadata.empty
      ),
      StructField("key", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField(
        "notBlank",
        DataTypes.BooleanType,
        nullable = true,
        Metadata.empty
      ),
      StructField(
        "default",
        DataTypes.StringType,
        nullable = true,
        Metadata.empty
      ),
      StructField(
        "description",
        DataTypes.StringType,
        nullable = true,
        Metadata.empty
      )
    )
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val datasourceType = DataSourceType.valueOf(
      CommonUtils.cleanQuote(ctx.value.getText).toUpperCase
    )
    val (sourceConnector, sinkConnector) =
      Utils.getDataTunnelConnector(datasourceType, datasourceType)

    val rows = if (ctx.SOURCE() != null) {
      if (sourceConnector == null) {
        return Seq.empty[Row]
      }
      DataTunnelUtils.getConnectorDoc("Source", sourceConnector.getOptionClass)
    } else if (ctx.SINK() != null) {
      if (sinkConnector == null) {
        return Seq.empty[Row]
      }
      DataTunnelUtils.getConnectorDoc("Sink", sinkConnector.getOptionClass)
    } else {
      val list: util.ArrayList[Row] = Lists.newArrayList()
      if (sourceConnector != null) {
        list.addAll(
          DataTunnelUtils.getConnectorDoc(
            "Source",
            sourceConnector.getOptionClass
          )
        )
      }
      if (sinkConnector != null) {
        list.addAll(
          DataTunnelUtils.getConnectorDoc("Sink", sinkConnector.getOptionClass)
        )
      }

      list
    }

    sparkSession.createDataFrame(rows, OUTPUT_TYPE).collect()
  }
}
