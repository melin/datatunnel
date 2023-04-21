package com.superior.datatunnel.core

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._

/**
 *
 * @author melin 2021/6/28 2:23 下午
 */
case class DataTunnelHelpCommand(sqlText: String, ctx: DataTunnelHelpCommand) extends LeafRunnableCommand with Logging{

  override def run(sparkSession: SparkSession): Seq[Row] = {

    Seq.empty[Row]
  }
}
