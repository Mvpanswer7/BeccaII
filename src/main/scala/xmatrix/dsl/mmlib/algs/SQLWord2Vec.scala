package xmatrix.dsl.mmlib.algs

import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import xmatrix.dsl.mmlib.SQLAlg

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 13/1/2018.
  */
class SQLWord2Vec extends SQLAlg with Functions {

  def train(df: DataFrame, path: String, params: Map[String, String]) = {
    val w2v = new Word2Vec()
    configureModel(w2v, params)
    val model = w2v.fit(df)
    model.write.overwrite().save(path)
  }

  def load(sparkSession: SparkSession, path: String, params: Map[String, String]) = {
    val model = Word2VecModel.load(path)
    // model.getVectors.collect().
    //   map(f => (f.getAs[String]("word"), f.getAs[DenseVector]("vector").toArray)).
    //   toMap
    model
  }

  def internal_predict(sparkSession: SparkSession, _model: Any, name: String) = {
    // val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[Map[String, Array[Double]]])
    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[Word2VecModel])
    val model_predict = model.value.getVectors.collect().
      map(f => (f.getAs[String]("word"), f.getAs[DenseVector]("vector").toArray)).
      toMap

    val model_find = model_predict.
      map(f => (f._1, model.value.findSynonymsArray(f._1, 10)))

    val f = (co: String) => {
      model_predict.get(co) match {
        case Some(vec) => vec.toSeq
        case None => null
      }

    }

    val f2 = (co: Seq[String]) => {
      co.map(f(_))
    }

    val f3 = (word: String, num: Int) => {
      model_find.get(word) match {
        case Some(vec) => vec.slice(0, num).toSeq
        case None => Array(("", 0.0))
      }
    }

    Map((name + "_array") -> f2, name -> f, (name + "_find") -> f3)
  }

  def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val res = internal_predict(sparkSession, _model, name)
    sparkSession.udf.register(name + "_array", res(name + "_array").asInstanceOf[Seq[String] => Seq[Seq[Double]]])
    sparkSession.udf.register(name + "_find", res(name + "_find").asInstanceOf[(String, Int) => Array[(String, Double)]])
    UserDefinedFunction(res(name), ArrayType(DoubleType), Some(Seq(StringType)))
  }
}
