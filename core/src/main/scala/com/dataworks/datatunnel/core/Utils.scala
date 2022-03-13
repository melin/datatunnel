package com.dataworks.datatunnel.core

import com.dataworks.datatunnel.common.util.CommonUtils
import com.dataworks.datatunnel.parser.DtunnelStatementParser.SparkOptionsContext
import com.google.common.collect.Maps

import java.util
import scala.collection.JavaConverters._

object Utils {

  def convertOptions(ctx: SparkOptionsContext): util.HashMap[String, String] = {
    val options: util.HashMap[String, String] = Maps.newHashMap()
    if (ctx != null) {
      for (entry <- ctx.optionVal().asScala) {
        val key = CommonUtils.cleanQuote(entry.optionKey().getText)
        val value = CommonUtils.cleanQuote(entry.optionValue().getText)
        options.put(key, value);
      }
    }

    options
  }
}
