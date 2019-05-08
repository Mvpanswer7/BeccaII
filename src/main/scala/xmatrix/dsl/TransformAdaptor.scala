package xmatrix.dsl
import xmatrix.dsl.parser.DSLSQLParser
import xmatrix.dsl.parser.DSLSQLParser._
import xmatrix.dsl.template.TemplateMerge
import xmatrix.dsl.transform.SQLTransform

class TransformAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {

  def evaluate(value: String) = {
    TemplateMerge.merge(value, scriptSQLExecListener.env().toMap)
  }

  override def parse(ctx: DSLSQLParser.SqlContext): Unit = {
    var tableName = ""
    var format = ""
    var path = ""
    var options = Map[String, String]()
    val owner = options.get("owner")
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: TableNameContext =>
          //TODO
          tableName = scriptSQLExecListener.resolveTableName(s.getText)
        case s: FormatContext =>
          format = s.getText
        case s: PathContext =>
          path = cleanStr(scriptSQLExecListener.resolveTableName(s.getText))
          path = evaluate(path)
        case s: ExpressionContext =>
          options += (cleanStr(s.identifier().getText) -> evaluate(cleanStr(s.STRING().getText)))
        case s: BooleanExpressionContext =>
          options += (cleanStr(s.expression().identifier().getText) -> evaluate(cleanStr(s.expression().STRING().getText)))
        case _ =>
      }
    }
    val df = scriptSQLExecListener.sparkSession.table(tableName)
    val sqlTrans = TransformMapping.findTrans(format)
    val newDf = sqlTrans.transform(df, path, options)
    scriptSQLExecListener.setLastSelectTable(null)
    newDf.createOrReplaceTempView(path)
  }
}

object TransformMapping {
  val mapping = Map[String, String](
    "LookUpHbase" -> "xmatrix.dsl.transform.trans.SQLLookUpHbase",
    "LookUpMysql" -> "xmatrix.dsl.transform.trans.SQLLookUpMysql"

  )

  def findTrans(name: String) = {
    val str = name.capitalize
    mapping.get(name.capitalize) match {
      case Some(clzz) =>
        Class.forName(clzz).newInstance().asInstanceOf[SQLTransform]
      case None =>
        if (!name.contains(".") && name.endsWith("InPlace")) {
          Class.forName(s"xmatrix.dsl.transform.trans.SQL${name}").newInstance().asInstanceOf[SQLTransform]
        } else {
          throw new RuntimeException(s"${name} is not found")
        }
    }
  }
}
