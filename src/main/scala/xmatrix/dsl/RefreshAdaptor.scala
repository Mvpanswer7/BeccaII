package xmatrix.dsl

import org.antlr.v4.runtime.misc.Interval
import xmatrix.dsl.parser.DSLSQLLexer
import xmatrix.dsl.parser.DSLSQLParser.SqlContext
import xmatrix.dsl.template.TemplateMerge

class RefreshAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {
  override def parse(ctx: SqlContext): Unit = {
    val input = ctx.start.getTokenSource().asInstanceOf[DSLSQLLexer]._input
    val start = ctx.start.getStartIndex()
    val stop = ctx.stop.getStopIndex()
    val interval = new Interval(start, stop)
    val originalText = input.getText(interval)
    val sql = TemplateMerge.merge(originalText, scriptSQLExecListener.env().toMap)
    scriptSQLExecListener.sparkSession.sql(sql).count()
    scriptSQLExecListener.setLastSelectTable(null)
  }
}
