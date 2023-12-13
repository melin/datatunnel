package com.superior.datatunnel.core

import com.gitee.melin.bee.util.SqlUtils
import io.github.melin.superior.parser.spark.SparkSqlPostProcessor
import io.github.melin.superior.parser.spark.antlr4.SparkSqlParser.{DatatunnelExprContext, DatatunnelHelpContext, SingleStatementContext}
import io.github.melin.superior.parser.spark.antlr4.{SparkSqlLexer, SparkSqlParser, SparkSqlParserBaseVisitor}
import org.antlr.v4.runtime.{CharStream, CharStreams, CodePointCharStream, CommonTokenStream, IntStream}
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.{Interval, ParseCancellationException}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.parser.{ParseErrorListener, ParseException, ParserInterface}
import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.datatunnel.sql.{DataTunnelExprCommand, DataTunnelHelpCommand}
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * huaixin 2021/12/27 2:48 PM
 */
class DataTunnelSqlParser (spark: SparkSession,
                      val delegate: ParserInterface) extends ParserInterface with Logging {

  override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
    val sql = StringUtils.trim(SqlUtils.cleanSqlComment(sqlText))
    val builder = new DtunnelAstBuilder(sql)
    builder.visit(parser.singleStatement()) match {
      case plan: LogicalPlan => plan
      case _ => delegate.parsePlan(sqlText)
    }
  }

  protected def parse[T](command: String)(toResult: SparkSqlParser => T): T = {
    val lexer = new SparkSqlLexer(new UpperCaseCharStream(CharStreams.fromString(command)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new SparkSqlParser(tokenStream)
    parser.addParseListener(new SparkSqlPostProcessor())
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)

    try {
      try {
        // first, try parsing with potentially faster SLL mode
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      }
      catch {
        case e: ParseCancellationException =>
          // if we fail, parse with LL mode
          tokenStream.seek(0) // rewind input stream
          parser.reset()

          // Try Again.
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    }
    catch {
      case e: ParseException if e.command.isDefined =>
        throw e
      case e: ParseException =>
        throw e.withCommand(command)
      case e: AnalysisException =>
        val position = Origin(e.line, e.startPosition)
        throw new ParseException(Option(command), e.message, position, position,
          e.errorClass, e.messageParameters)
    }
  }

  /** Creates/Resolves DataType for a given SQL string. */
  override def parseDataType(sqlText: String): DataType = {
    delegate.parseDataType(sqlText)
  }

  /** Creates Expression for a given SQL string. */
  override def parseExpression(sqlText: String): Expression = {
    delegate.parseExpression(sqlText)
  }

  /** Creates TableIdentifier for a given SQL string. */
  override def parseTableIdentifier(sqlText: String): TableIdentifier = {
    delegate.parseTableIdentifier(sqlText)
  }

  /** Creates FunctionIdentifier for a given SQL string. */
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
    delegate.parseFunctionIdentifier(sqlText)
  }

  /** Creates a multi-part identifier for a given SQL string */
  override def parseMultipartIdentifier(sqlText: String): Seq[String] = {
    delegate.parseMultipartIdentifier(sqlText)
  }

  /**
   * Creates StructType for a given SQL string, which is a comma separated list of field
   * definitions which will preserve the correct Hive metadata.
   */
  override def parseTableSchema(sqlText: String): StructType = {
    delegate.parseTableSchema(sqlText)
  }

  override def parseQuery(sqlText: String): LogicalPlan = {
    delegate.parseQuery(sqlText)
  }
}

class DtunnelAstBuilder(val sqlText: String) extends SparkSqlParserBaseVisitor[AnyRef] with Logging {

  override def visitDatatunnelExpr(ctx: DatatunnelExprContext): LogicalPlan = withOrigin(ctx) {
    val maskSql = DataTunnelUtils.maskSql(sqlText)
    logInfo(s"Parsing command: $maskSql")
    DataTunnelExprCommand(sqlText, ctx: DatatunnelExprContext)
  }

  override def visitDatatunnelHelp(ctx: DatatunnelHelpContext): LogicalPlan = withOrigin(ctx) {
    DataTunnelHelpCommand(sqlText, ctx: DatatunnelHelpContext)
  }

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }
}

class UpperCaseCharStream(wrapped: CodePointCharStream) extends CharStream {
  override def consume(): Unit = wrapped.consume
  override def getSourceName(): String = wrapped.getSourceName
  override def index(): Int = wrapped.index
  override def mark(): Int = wrapped.mark
  override def release(marker: Int): Unit = wrapped.release(marker)
  override def seek(where: Int): Unit = wrapped.seek(where)
  override def size(): Int = wrapped.size

  override def getText(interval: Interval): String = wrapped.getText(interval)

  override def LA(i: Int): Int = {
    val la = wrapped.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
}