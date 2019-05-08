package xmatrix.dsl

import xmatrix.dsl.mmlib.SQLAlg
import xmatrix.dsl.parser.DSLSQLParser._
import xmatrix.dsl.template.TemplateMerge

class TrainAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {

  def evaluate(value: String) = {
    TemplateMerge.merge(value, scriptSQLExecListener.env().toMap)
  }

  override def parse(ctx: SqlContext): Unit = {
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
          path = cleanStr(s.getText)
          path = evaluate(path)
          path = withPathPrefix("", path)
          //path = withPathPrefix("", path)
        case s: ExpressionContext =>
          options += (cleanStr(s.identifier().getText) -> evaluate(cleanStr(s.STRING().getText)))
        case s: BooleanExpressionContext =>
          options += (cleanStr(s.expression().identifier().getText) -> evaluate(cleanStr(s.expression().STRING().getText)))
        case _ =>
      }
    }
    val df = scriptSQLExecListener.sparkSession.table(tableName)
    val sqlAlg = MLMapping.findAlg(format)
    sqlAlg.train(df, path, options)
    scriptSQLExecListener.setLastSelectTable(null)
  }
}

object MLMapping {
  val mapping = Map[String, String](
    "Word2vec" -> "xmatrix.dsl.mmlib.algs.SQLWord2Vec",
    "NaiveBayes" -> "xmatrix.dsl.mmlib.algs.SQLNaiveBayes",
    "RandomForest" -> "xmatrix.dsl.mmlib.algs.SQLRandomForest",
    "GBTRegressor" -> "xmatrix.dsl.mmlib.algs.SQLGBTRegressor",
    "LDA" -> "xmatrix.dsl.mmlib.algs.SQLLDA",
    "KMeans" -> "xmatrix.dsl.mmlib.algs.SQLKMeans",
    "FPGrowth" -> "xmatrix.dsl.mmlib.algs.SQLFPGrowth",
    "StringIndex" -> "xmatrix.dsl.mmlib.algs.SQLStringIndex",
    "GBTs" -> "xmatrix.dsl.mmlib.algs.SQLGBTs",
    "LSVM" -> "xmatrix.dsl.mmlib.algs.SQLLSVM",
    "HashTfIdf" -> "xmatrix.dsl.mmlib.algs.SQLHashTfIdf",
    "TfIdf" -> "xmatrix.dsl.mmlib.algs.SQLTfIdf",
    "LogisticRegressor" -> "xmatrix.dsl.mmlib.algs.SQLLogisticRegression",
    "RowMatrix" -> "xmatrix.dsl.mmlib.algs.SQLRowMatrix",
    "PageRank" -> "xmatrix.dsl.mmlib.algs.SQLPageRank",
    "TensorFlow" -> "xmatrix.dsl.mmlib.algs.SQLTensorFlow",
    "StandardScaler" -> "xmatrix.dsl.mmlib.algs.SQLStandardScaler",
    "SKLearn" -> "xmatrix.dsl.mmlib.algs.SQLSKLearn",
    "DicOrTableToArray" -> "xmatrix.dsl.mmlib.algs.SQLDicOrTableToArray",
    "TableToMap" -> "xmatrix.dsl.mmlib.algs.SQLTableToMap",
    "DL4J" -> "xmatrix.dsl.mmlib.algs.SQLDL4J",
    "TokenExtract" -> "xmatrix.dsl.mmlib.algs.SQLTokenExtract",
    "TokenAnalysis" -> "xmatrix.dsl.mmlib.algs.SQLTokenAnalysis",
    "TfIdfInPlace" -> "xmatrix.dsl.mmlib.algs.SQLTfIdfInPlace",
    "Word2VecInPlace" -> "xmatrix.dsl.mmlib.algs.SQLWord2VecInPlace",
    "AutoFeature" -> "xmatrix.dsl.mmlib.algs.SQLAutoFeature",
    "RateSampler" -> "xmatrix.dsl.mmlib.algs.SQLRateSampler",
    "ScalerInPlace" -> "xmatrix.dsl.mmlib.algs.SQLScalerInPlace",
    "NormalizeInPlace" -> "xmatrix.dsl.mmlib.algs.SQLNormalizeInPlace",
    "PythonAlg" -> "xmatrix.dsl.mmlib.algs.SQLPythonAlg",
    "OpenCVImage" -> "xmatrix.dsl.mmlib.algs.processing.SQLOpenCVImage",
    "JavaImage" -> "xmatrix.dsl.mmlib.algs.processing.SQLJavaImage",
    "Discretizer" -> "xmatrix.dsl.mmlib.algs.SQLDiscretizer",
    "CorpusExplainInPlace" -> "xmatrix.dsl.mmlib.algs.SQLCorpusExplainInPlace"

  )

  def findAlg(name: String) = {
    mapping.get(name.capitalize) match {
      case Some(clzz) =>
        Class.forName(clzz).newInstance().asInstanceOf[SQLAlg]
      case None =>
        if (!name.contains(".") && name.endsWith("InPlace")) {
          Class.forName(s"xmatrix.dsl.mmlib.algs.SQL${name}").newInstance().asInstanceOf[SQLAlg]
        } else {
          throw new RuntimeException(s"${name} is not found")
        }
    }
  }
}
