package xmatrix.dsl.mmlib

/**
  * Created by iodone on {18-6-12}.
  */

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction

trait SQLAlg {
  def train(df: DataFrame, path: String, params: Map[String, String]): Unit

  def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any

  def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction

}

