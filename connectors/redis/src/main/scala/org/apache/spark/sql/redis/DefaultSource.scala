package org.apache.spark.sql.redis

import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]
  ): BaseRelation = {
    new RedisSourceRelation(sqlContext, parameters)
  }

  /** Creates a new relation by saving the data to Redis
    */
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame
  ): BaseRelation = {
    val relation = new RedisSourceRelation(sqlContext, parameters)
    relation.insert(data, overwrite = true)
    relation
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType
  ): BaseRelation =
    new RedisSourceRelation(sqlContext, parameters)
}
