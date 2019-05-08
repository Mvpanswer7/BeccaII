package xmatrix.dsl.mmlib.algs

import org.apache.spark.ml.fpm.{FPGrowth, FPGrowthModel}
import org.apache.spark.ml.linalg.SQLDataTypes._
import xmatrix.dsl.mmlib.SQLAlg
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{ArrayType, IntegerType, ObjectType, StringType}

class SQLFPGrowth extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    val rfc = new FPGrowth()
    configureModel(rfc, params)
    val model = rfc.fit(df)
    model.write.overwrite().save(path)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    val model = FPGrowthModel.load(path)
    model
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val model = _model.asInstanceOf[FPGrowthModel]
    val rules: Array[(Seq[String], Seq[String])] = model.associationRules.select("antecedent", "consequent")
      .rdd.map(r => (r.getSeq(0), r.getSeq(1)))
      .collect().asInstanceOf[Array[(Seq[String], Seq[String])]]
    val brRules = sparkSession.sparkContext.broadcast(rules)


    val f = (items: Seq[String]) => {
      if (items != null) {
        val itemset = items.toSet
        brRules.value.flatMap(rule =>
          if (items != null && rule._1.forall(item => itemset.contains(item))) {
            rule._2.filter(item => !itemset.contains(item))
          } else {
            Seq.empty
          }).distinct
      } else {
        Seq.empty
      }
    }
    UserDefinedFunction(f, ArrayType(StringType), Some(Seq(ArrayType(StringType))))
  }
}
