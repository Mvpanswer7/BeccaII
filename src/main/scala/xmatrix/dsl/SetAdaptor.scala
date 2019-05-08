package xmatrix.dsl

import xmatrix.dsl.parser.DSLSQLParser._
import xmatrix.common.shell.ShellCommand


class SetAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {
  override def parse(ctx: SqlContext): Unit = {
    var key = ""
    var value = ""
    var command = ""
    var original_command = ""
    var option = Map[String, String]()
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>

      ctx.getChild(tokenIndex) match {
        case s: SetKeyContext =>
          key = s.getText
        case s: SetValueContext =>
          original_command = s.getText
          if (s.quotedIdentifier() != null && s.quotedIdentifier().BACKQUOTED_IDENTIFIER() != null) {
            command = cleanStr(s.getText)
          } else if (s.qualifiedName() != null && s.qualifiedName().identifier() != null) {
            command = cleanStr(s.getText)
          }
          else {
            command = original_command
          }
        case s: ExpressionContext =>
          option += (cleanStr(s.identifier().getText) -> cleanStr(s.STRING().getText))
        case s: BooleanExpressionContext =>
          option += (cleanStr(s.expression().identifier().getText) -> cleanStr(s.expression().STRING().getText))
        case _ =>
      }
    }

    option.get("type") match {
      case Some("sql") =>
        val resultHead = scriptSQLExecListener.sparkSession.sql(command).collect().headOption
        if (resultHead.isDefined) {
          value = resultHead.get.get(0).toString
        }
      case Some("shell") =>
        value = ShellCommand.exec(command).trim
      case Some("conf") =>
        scriptSQLExecListener.sparkSession.sql(s""" set ${key} = ${original_command} """)
      case _ =>
        value = cleanStr(command)
    }

    scriptSQLExecListener.addEnv(key, value)
    scriptSQLExecListener.setLastSelectTable(null)
    //scriptSQLExecListener.sparkSession.sql(ctx.)
  }
}
