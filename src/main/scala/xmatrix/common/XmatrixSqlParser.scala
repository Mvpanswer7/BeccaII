package xmatrix.common

import com.alibaba.druid.sql.ast.expr._
import com.alibaba.druid.sql.ast.statement._
import com.alibaba.druid.sql.ast.{SQLObject, SQLStatement}
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser
import com.alibaba.druid.sql.dialect.mysql.visitor.{MySqlOutputVisitor, MySqlSchemaStatVisitor}
import xmatrix.dsl.ScriptSQLExec

import scala.collection.JavaConversions._

object XmatrixSqlParser {
  def getTableName(sql: String) = {
    val parser = new MySqlStatementParser(sql)
    val sqlStatement = parser.parseStatement()
    val visitor = new MySqlSchemaStatVisitor()
    sqlStatement.accept(visitor)
    visitor.getTables.keys.last.toString
  }

  def replaceTableNames(sql: String, suffix: String): String = {
    val parser = new MySqlStatementParser(sql)
    val sqlStatement = parser.parseStatement()

    if (sqlStatement.isInstanceOf[SQLSelectStatement]) {
      sqlStatement.asInstanceOf[SQLSelectStatement].getSelect.getQuery match {

        case _: MySqlSelectQueryBlock =>
          buildSelect(sqlStatement.asInstanceOf[SQLSelectStatement].getSelect.getQuery.asInstanceOf[MySqlSelectQueryBlock], suffix)

        case _: SQLUnionQuery =>
          solveUnionQuery(sqlStatement.asInstanceOf[SQLSelectStatement].getSelect.getQuery.asInstanceOf[SQLUnionQuery], suffix)
      }
    }
    val out = new java.lang.StringBuilder("")
    val visitor = new MySqlOutputVisitor(out)
    sqlStatement.accept(visitor)
    out.toString
  }

  private def solveUnionQuery(query: SQLUnionQuery, suffix: String) : Unit = {
    query.getLeft match {
      case _: SQLUnionQuery =>
        solveUnionQuery(query.getLeft.asInstanceOf[SQLUnionQuery], suffix)
      case _: MySqlSelectQueryBlock =>
        buildSelect(query.getLeft.asInstanceOf[MySqlSelectQueryBlock], suffix)
      case _ =>
    }
    query.getRight match {
      case _: SQLUnionQuery =>
        solveUnionQuery(query.getRight.asInstanceOf[SQLUnionQuery], suffix)
      case _: MySqlSelectQueryBlock =>
        buildSelect(query.getRight.asInstanceOf[MySqlSelectQueryBlock], suffix)
      case _ =>
    }
  }

  def buildSelect(sqlSelect: MySqlSelectQueryBlock, replaceName: String): Unit = {
    val query = sqlSelect
    replaceTableName(query.getFrom, replaceName)
    replaceTableName(query.getWhere, replaceName)
    query.getSelectList foreach {
      f =>
        replaceTableName(f, replaceName)
    }
    query.getGroupBy match {
      case null =>
      case _ =>
        query.getGroupBy.getItems match {
          case null =>
          case _ =>
            query.getGroupBy.getItems foreach {
              f =>
                replaceTableName(f, replaceName)
            }
        }
    }
    query.getOrderBy match {
      case null =>
      case _ =>
        query.getOrderBy.getItems match {
          case null =>
          case _ =>
            query.getOrderBy.getItems foreach {
              f =>
                replaceTableName(f.getExpr, replaceName)
            }
        }
    }
  }

  private def replaceTableName(sqlTableSource: SQLObject, suffix: String): Unit = {
    sqlTableSource match {
      case _: SQLExprTableSource =>
        val leftExpr = sqlTableSource.asInstanceOf[SQLExprTableSource].getExpr.asInstanceOf[SQLIdentifierExpr]
        if(ScriptSQLExec.tmpTables.contains(leftExpr.getName + suffix))
          leftExpr.setName(leftExpr.getName + suffix)
      case _:SQLJoinTableSource =>
        val left = sqlTableSource.asInstanceOf[SQLJoinTableSource].getLeft
        val right = sqlTableSource.asInstanceOf[SQLJoinTableSource].getRight
        replaceTableName(left, suffix)
        replaceTableName(right, suffix)

        val joinOn = sqlTableSource.asInstanceOf[SQLJoinTableSource].getCondition
        replaceTableName(joinOn, suffix)
      case _: SQLBinaryOpExpr =>
        val binaryOpExpr = sqlTableSource.asInstanceOf[SQLBinaryOpExpr]
        val left = binaryOpExpr.getLeft
        val right = binaryOpExpr.getRight
        replaceTableName(left, suffix)
        replaceTableName(right, suffix)
      case _: SQLIdentifierExpr =>
        val ident = sqlTableSource.asInstanceOf[SQLIdentifierExpr].getName
      case _: SQLQueryExpr =>
        buildSelect(sqlTableSource.asInstanceOf[SQLQueryExpr].getSubQuery.getQuery.asInstanceOf[MySqlSelectQueryBlock], suffix)
      case _: SQLLateralViewTableSource =>
        val tableSource = sqlTableSource.asInstanceOf[SQLLateralViewTableSource].getTableSource
        if(tableSource.isInstanceOf[SQLSubqueryTableSource]) {
          buildSelect(tableSource.asInstanceOf[SQLSubqueryTableSource].getSelect.getQuery.asInstanceOf[MySqlSelectQueryBlock], suffix)
        } else {
          val leftExpr = sqlTableSource.asInstanceOf[SQLLateralViewTableSource].getTableSource.asInstanceOf[SQLExprTableSource].getExpr.asInstanceOf[SQLIdentifierExpr]
          if(ScriptSQLExec.tmpTables.contains(leftExpr.getName + suffix))
            leftExpr.setName(leftExpr.getName + suffix)
        }
      case _: SQLPropertyExpr =>
        if(ScriptSQLExec.tmpTables.contains(sqlTableSource.asInstanceOf[SQLPropertyExpr].getOwner + suffix))
          sqlTableSource.asInstanceOf[SQLPropertyExpr].setOwner(sqlTableSource.asInstanceOf[SQLPropertyExpr].getOwner + suffix)
      case _: SQLSelectItem =>
        if(sqlTableSource.asInstanceOf[SQLSelectItem].getExpr.isInstanceOf[SQLPropertyExpr]) {
          if(ScriptSQLExec.tmpTables.contains(sqlTableSource.asInstanceOf[SQLSelectItem].getExpr.asInstanceOf[SQLPropertyExpr].getOwner + suffix))
          sqlTableSource.asInstanceOf[SQLSelectItem].getExpr.asInstanceOf[SQLPropertyExpr].setOwner(sqlTableSource.asInstanceOf[SQLSelectItem].getExpr.asInstanceOf[SQLPropertyExpr].getOwner + suffix)
        } else {
          sqlTableSource.toString
        }
      case _: SQLSubqueryTableSource =>
        buildSelect(sqlTableSource.asInstanceOf[SQLSubqueryTableSource].getSelect.getQuery.asInstanceOf[MySqlSelectQueryBlock], suffix)
      case _: SQLInSubQueryExpr =>
        buildSelect(sqlTableSource.asInstanceOf[SQLInSubQueryExpr].getSubQuery.getQuery.asInstanceOf[MySqlSelectQueryBlock], suffix)
      case _: SQLUnionQueryTableSource =>
        solveUnionQuery(sqlTableSource.asInstanceOf[SQLUnionQueryTableSource].getUnion, suffix)
      case _: SQLInListExpr =>
        //println(sqlTableSource.asInstanceOf[SQLInListExpr].toString)
      case _: SQLExistsExpr =>
        buildSelect(sqlTableSource.asInstanceOf[SQLExistsExpr].getSubQuery.getQuery.asInstanceOf[MySqlSelectQueryBlock], suffix)
      case _ =>
    }
  }

}
