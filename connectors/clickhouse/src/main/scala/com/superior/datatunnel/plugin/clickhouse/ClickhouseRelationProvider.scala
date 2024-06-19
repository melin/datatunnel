package com.superior.datatunnel.plugin.clickhouse

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{
  BaseRelation,
  CreatableRelationProvider,
  DataSourceRegister,
  RelationProvider
}

class ClickhouseRelationProvider
    extends CreatableRelationProvider
    with RelationProvider
    with DataSourceRegister {

  override def shortName(): String = "clickhouse"

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame
  ): BaseRelation = {
    return null
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]
  ): BaseRelation = {
    return null
  }
}
