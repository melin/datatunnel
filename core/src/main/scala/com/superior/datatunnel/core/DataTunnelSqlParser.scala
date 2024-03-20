package com.superior.datatunnel.core

import com.gitee.melin.bee.util.SqlUtils
import io.github.melin.superior.common.relational.io.ExportTable
import io.github.melin.superior.parser.spark.{SparkSqlHelper, SparkSqlPostProcessor}
import io.github.melin.superior.parser.spark.antlr4.SparkSqlParser._
import io.github.melin.superior.parser.spark.antlr4.{SparkSqlLexer, SparkSqlParser, SparkSqlParserBaseVisitor}
import org.antlr.v4.runtime.{CharStream, CharStreams, CodePointCharStream, CommonTokenStream, IntStream, misc}
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
import org.apache.spark.sql.datatunnel.sql.{DataTunnelExprCommand, DataTunnelHelpCommand, DistCpCommand, ExportTableCommand}
import org.apache.spark.sql.types.{DataType, StructType}

import scala.collection.JavaConverters._

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
    val maskSql = DataTunnelUtils.maskDataTunnelSql(sqlText)
    logInfo(s"Datatunnel SQL: $maskSql")
    DataTunnelExprCommand(sqlText, ctx: DatatunnelExprContext)
  }

  override def visitDistCpExpr(ctx: DistCpExprContext): LogicalPlan = withOrigin(ctx) {
    DistCpCommand(sqlText, ctx: DistCpExprContext)
  }

  override def visitDatatunnelHelp(ctx: DatatunnelHelpContext): LogicalPlan = withOrigin(ctx) {
    logInfo(s"Datatunnel SQL: $sqlText")
    DataTunnelHelpCommand(sqlText, ctx: DatatunnelHelpContext)
  }

  override def visitExportTable(ctx: ExportTableContext): LogicalPlan = withOrigin(ctx) {
    logInfo(s"export table SQL: $sqlText")
    val exportTable = SparkSqlHelper.parseStatement(sqlText).asInstanceOf[ExportTable]
    val subqueryAlias =
      if (ctx.ctes() != null) {
        withCTE(ctx.ctes())
      } else {
        Seq[(String, String)]()
      }

    ExportTableCommand(exportTable, subqueryAlias)
  }

  override def visitNamedQuery(ctx: NamedQueryContext): (String, String) = withOrigin(ctx) {
    val subQuery: String = {
      val queryContext = ctx.query()
      val s = queryContext.start.getStartIndex
      val e = queryContext.stop.getStopIndex
      val interval = new misc.Interval(s, e)
      val viewSql = ctx.start.getInputStream.getText(interval)
      viewSql
    }
    (ctx.name.getText, subQuery)
  }

  private def withCTE(ctx: CtesContext): Seq[(String, String)] = {
    val ctes = ctx.namedQuery.asScala.map { nCtx =>
      visitNamedQuery(nCtx)
    }
    // Check for duplicate names.
    val duplicates = ctes.groupBy(_._1).filter(_._2.size > 1).keys
    if (duplicates.nonEmpty) {
      throw new ParseException(
        s"CTE definition can't have duplicate names: ${duplicates.mkString("'", "', '", "'")}.",
        ctx)
    }
    ctes
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