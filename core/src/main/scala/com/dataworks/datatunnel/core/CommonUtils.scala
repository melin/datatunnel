package com.dataworks.datatunnel.core

import com.dataworks.datatunnel.parser.DataxStatementParser.SparkOptionsContext
import com.google.common.collect.Maps
import org.apache.commons.lang3.StringUtils

import java.util
import scala.collection.JavaConverters._

object CommonUtils {

  def cleanQuote(value: String): String = {
    val result = if (StringUtils.startsWith(value, "'") && StringUtils.endsWith(value, "'")) {
      StringUtils.substring(value, 1, -1);
    } else if (StringUtils.startsWith(value, "\"") && StringUtils.endsWith(value, "\"")) {
      StringUtils.substring(value, 1, -1);
    } else {
      value;
    }

    result.trim
  }

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
