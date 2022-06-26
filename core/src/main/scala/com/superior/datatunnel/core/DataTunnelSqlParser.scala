package com.superior.datatunnel.core

import com.superior.datatunnel.common.util.CommonUtils
import com.superior.datatunnel.parser.DtunnelStatementParser.{DtunnelExprContext, PassThroughContext, SingleStatementContext}
import com.superior.datatunnel.parser.{DtunnelStatementBaseVisitor, DtunnelStatementLexer, DtunnelStatementParser}
import org.antlr.v4.runtime.{CharStream, CharStreams, CodePointCharStream, CommonTokenStream, IntStream}
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.{Interval, ParseCancellationException}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.parser.{ParseErrorListener, ParseException, ParserInterface, PostProcessor}
import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * huaixin 2021/12/27 2:48 PM
 */
class DataTunnelSqlParser (spark: SparkSession,
                      val delegate: ParserInterface) extends ParserInterface with Logging {

  private val builder = new DtunnelAstBuilder()

  override def parsePlan(sqlText: String): LogicalPlan = {
    val sql = CommonUtils.cleanSqlComment(sqlText)
    if (StringUtils.startsWithIgnoreCase(sqlText, "datatunnel")) {
      parse(sql) { parser => builder.visit(parser.singleStatement()) }.asInstanceOf[LogicalPlan]
    } else {
      val parsedPlan = delegate.parsePlan(sqlText)
      parsedPlan match {
        case plan: LogicalPlan => plan
        case _ => delegate.parsePlan(sql)
      }
    }
  }

  override def parseQuery(sqlText: String): LogicalPlan = {
    val sql = CommonUtils.cleanSqlComment(sqlText)
    if (StringUtils.startsWithIgnoreCase(sqlText, "datatunnel")) {
      parse(sql) { parser => builder.visit(parser.singleStatement()) }.asInstanceOf[LogicalPlan]
    } else {
      val parsedPlan = delegate.parseQuery(sqlText)
      parsedPlan match {
        case plan: LogicalPlan => plan
        case _ => delegate.parseQuery(sql)
      }
    }
  }

  protected def parse[T](command: String)(toResult: DtunnelStatementParser => T): T = {
    logInfo(s"Parsing command: $command")

    val lexer = new DtunnelStatementLexer(new UpperCaseCharStream(CharStreams.fromString(command)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new DtunnelStatementParser(tokenStream)
    parser.addParseListener(PostProcessor)
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
}

class DtunnelAstBuilder extends DtunnelStatementBaseVisitor[AnyRef] {

  override def visitDtunnelExpr(ctx: DtunnelExprContext): LogicalPlan = withOrigin(ctx) {
    DataTunnelExprCommand(ctx: DtunnelExprContext)
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