package xmatrix.dsl

import xmatrix.dsl.parser.DSLSQLParser._

class ConnectAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {
  override def parse(ctx: SqlContext): Unit = {

    var option = Map[String, String]()

    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: FormatContext =>
          option += ("format" -> s.getText)

        case s: ExpressionContext =>
          option += (cleanStr(s.identifier().getText) -> cleanStr(s.STRING().getText))
        case s: BooleanExpressionContext =>
          option += (cleanStr(s.expression().identifier().getText)-> cleanStr(s.expression().STRING().getText))
        case s: DbContext =>
          ScriptSQLExec.options(s.getText, option)
        case _ =>

      }
    }
    //scriptSQLExecListener.setLastSelectTable(null)
  }
}
