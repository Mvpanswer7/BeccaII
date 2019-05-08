package xmatrix.dsl

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import org.antlr.v4.runtime.tree._
import org.antlr.v4.runtime._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.internal.SQLConf
import xmatrix.dsl.parser._
import xmatrix.dsl.parser.DSLSQLParser._

import scala.collection.mutable
import org.apache.log4j.Logger
import xmatrix.common.XmatrixSqlParser

class ScriptSQLExec

object ScriptSQLExec {
  val dbMapping = new ConcurrentHashMap[String, Map[String, String]]()
  val _tmpTables = new mutable.HashSet[String]()

  def tmpTables =
    _tmpTables

  def addTmpTable(tableName: String) =
    _tmpTables.add(tableName)

  def options(name: String, _options: Map[String, String]) = {
    dbMapping.put(name, _options)
  }

  def clear = dbMapping.clear()


  def extractSql(xql: String) = {
    val sqlStatements = xql.split(";").filter(x => x.trim.toLowerCase().startsWith("select"))
    sqlStatements.map(x => {
      val reg1 = """as(\s+)[\w]*\w[\w.]*(\s+)?;"""
      val reg2 = """options(\s+)[^,]+$"""
      val x1 = x.replaceAll(reg2,"")
      val x2 = x1 + ";" replaceAll(reg1,"")
      x2
    })
  }

  def validateSql(sql: String): Unit = {
    val conf = new SQLConf().copy(SQLConf.CASE_SENSITIVE -> false)
    val parser = new CatalystSqlParser(conf)
    val x = parser.parsePlan(sql)
  }

  def syntaxCheck(input: String) = {
    val loadLexer = new DSLSQLLexer(new ANTLRInputStream(input))
    val tokens = new CommonTokenStream(loadLexer)
    val parser = new DSLSQLParser(tokens)
    parser.removeErrorListeners()
    parser.addErrorListener(ThrowErrorListener.INSTANCE)
    parser.statement()

    val sqlStatements = extractSql(input)
    sqlStatements.foreach(validateSql(_))

  }


  def xqlsyntaxCheck(input: String) = {
    val loadLexer = new DSLSQLLexer(new ANTLRInputStream(input))
    val tokens = new CommonTokenStream(loadLexer)
    val parser = new DSLSQLParser(tokens)
    parser.removeErrorListeners()
    parser.addErrorListener(ThrowErrorListener.INSTANCE)
    parser.statement()

  }

  def parse(input: String, listener: DSLSQLListener) = {
    val stat = xqlsyntaxCheck(input)
    ParseTreeWalker.DEFAULT.walk(listener, stat)
  }

}

class ScriptSQLExecListener(_sparkSession: SparkSession, _defaultPathPrefix: String, _allPathPrefix: Map[String, String], _tmpTableSuf: String = "", checkpointEnable: Boolean = false, jobGroupId: String = "") extends DSLSQLListener {
  val logger = Logger.getLogger(classOf[ScriptSQLExecListener])

  private val _env = new scala.collection.mutable.HashMap[String, String]
  private val lastSelectTable = new AtomicReference[String]()
  private val _groupId = jobGroupId

  def setLastSelectTable(table: String) = {
    lastSelectTable.set(table)
  }

  def getLastSelectTable() = {
    if (lastSelectTable.get() == null) None else Some(lastSelectTable.get())
  }

  def addEnv(k: String, v: String) = {
    _env(k) = v
    this
  }

  def groupId = _groupId

  def checkpoint =
    checkpointEnable

  def env() = _env

  def sparkSession = _sparkSession

  def pathPrefix(owner: Option[String]): String = {

    if (_allPathPrefix != null && _allPathPrefix.nonEmpty && owner.isDefined) {
      val pathPrefix = _allPathPrefix.get(owner.get)
      if (pathPrefix.isDefined && pathPrefix.get.endsWith("/")) {
        return pathPrefix.get
      } else {
        return pathPrefix.get + "/"
      }
    } else if (_defaultPathPrefix != null && _defaultPathPrefix.nonEmpty) {
      if (_defaultPathPrefix.endsWith("/")) {
        return _defaultPathPrefix
      } else {
        return _defaultPathPrefix + "/"
      }
    } else {
      return ""
    }
  }

  def resolveName(name: String) = {
    if("".equals(_tmpTableSuf)) {
      name.replace("`","")
    } else {
      name.replace("`","") + "_" + _tmpTableSuf
    }
  }

  def resolveTableName(tableName: String) = {
    val name = resolveName(tableName)
    ScriptSQLExec.addTmpTable(name)
    name
  }

  def dropTempTable() = {
    ScriptSQLExec.tmpTables foreach {
      table =>
        sparkSession.catalog.dropTempView(table)
    }
  }

  def replaceTableName(sql: String) = {
    if(!"".equals(_tmpTableSuf) ) {
      logger.info(s"Checking sql: $sql")
      val newSql = XmatrixSqlParser.replaceTableNames(sql,  "_" + _tmpTableSuf)
      newSql
    } else {
      sql
    }
  }

  override def exitSql(ctx: SqlContext): Unit = {
    ctx.getChild(0).getText.toLowerCase() match {
      case "load" =>
        new LoadAdaptor(this).parse(ctx)

      case "select" =>
        new SelectAdaptor(this).parse(ctx)

      case "save" =>
        new SaveAdaptor(this).parse(ctx)

      case "connect" =>
        new ConnectAdaptor(this).parse(ctx)
      case "create" =>
        new CreateAdaptor(this).parse(ctx)
      case "insert" =>
        new InsertAdaptor(this).parse(ctx)
      case "drop" =>
        new DropAdaptor(this).parse(ctx)
      case "refresh" =>
        new RefreshAdaptor(this).parse(ctx)
      case "set" =>
        new SetAdaptor(this).parse(ctx)
      case "register" =>
        new RegisterAdaptor(this).parse(ctx)
      case "train" =>
        new TrainAdaptor(this).parse(ctx)
      case "trans" =>
        new TransformAdaptor(this).parse(ctx)
    }

  }

  override def enterStatement(ctx: StatementContext): Unit = {}

  override def exitStatement(ctx: StatementContext): Unit = {}

  override def enterSql(ctx: SqlContext): Unit = {}

  override def enterFormat(ctx: FormatContext): Unit = {}

  override def exitFormat(ctx: FormatContext): Unit = {}

  override def enterPath(ctx: PathContext): Unit = {}

  override def exitPath(ctx: PathContext): Unit = {}

  override def enterTableName(ctx: TableNameContext): Unit = {}

  override def exitTableName(ctx: TableNameContext): Unit = {}

  override def enterCol(ctx: ColContext): Unit = {}

  override def exitCol(ctx: ColContext): Unit = {}

  override def enterQualifiedName(ctx: QualifiedNameContext): Unit = {}

  override def exitQualifiedName(ctx: QualifiedNameContext): Unit = {}

  override def enterIdentifier(ctx: IdentifierContext): Unit = {}

  override def exitIdentifier(ctx: IdentifierContext): Unit = {}

  override def enterStrictIdentifier(ctx: StrictIdentifierContext): Unit = {}

  override def exitStrictIdentifier(ctx: StrictIdentifierContext): Unit = {}

  override def enterQuotedIdentifier(ctx: QuotedIdentifierContext): Unit = {}

  override def exitQuotedIdentifier(ctx: QuotedIdentifierContext): Unit = {}

  override def visitTerminal(node: TerminalNode): Unit = {}

  override def visitErrorNode(node: ErrorNode): Unit = {}

  override def exitEveryRule(ctx: ParserRuleContext): Unit = {}

  override def enterEveryRule(ctx: ParserRuleContext): Unit = {}

  override def enterEnder(ctx: EnderContext): Unit = {}

  override def exitEnder(ctx: EnderContext): Unit = {}

  override def enterExpression(ctx: ExpressionContext): Unit = {}

  override def exitExpression(ctx: ExpressionContext): Unit = {}

  override def enterBooleanExpression(ctx: BooleanExpressionContext): Unit = {}

  override def exitBooleanExpression(ctx: BooleanExpressionContext): Unit = {}

  override def enterDb(ctx: DbContext): Unit = {}

  override def exitDb(ctx: DbContext): Unit = {}

  override def enterOverwrite(ctx: OverwriteContext): Unit = {}

  override def exitOverwrite(ctx: OverwriteContext): Unit = {}

  override def enterAppend(ctx: AppendContext): Unit = {}

  override def exitAppend(ctx: AppendContext): Unit = {}

  override def enterErrorIfExists(ctx: ErrorIfExistsContext): Unit = {}

  override def exitErrorIfExists(ctx: ErrorIfExistsContext): Unit = {}

  override def enterIgnore(ctx: IgnoreContext): Unit = {}

  override def exitIgnore(ctx: IgnoreContext): Unit = {}

  override def enterFunctionName(ctx: FunctionNameContext): Unit = {}

  override def exitFunctionName(ctx: FunctionNameContext): Unit = {}

  override def enterSetValue(ctx: SetValueContext): Unit = {}

  override def exitSetValue(ctx: SetValueContext): Unit = {}

  override def enterSetKey(ctx: SetKeyContext): Unit = {}

  override def exitSetKey(ctx: SetKeyContext): Unit = {}

}

