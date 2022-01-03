package com.dataworks.datatunnel.sftp.spark

import org.apache.spark.sql.DataFrameWriter

/**
 * Created by bagopalan on 9/16/18.
 */
object Constants {

  val xmlClass = "com.databricks.com.dataworker.datax.sftp.spark.xml"
  val xmlRowTag = "rowTag"
  val xmlRootTag = "rootTag"

  implicit class ImplicitDataFrameWriter[T](dataFrameWriter: DataFrameWriter[T]) {

    /**
     * Adds an output option for the underlying data source if the option has a value.
     */
    def optionNoNull(key: String, optionValue: Option[String]): DataFrameWriter[T] = {
      optionValue match {
        case Some(_) => dataFrameWriter.option(key, optionValue.get)
        case None => dataFrameWriter
      }
    }
  }
}
