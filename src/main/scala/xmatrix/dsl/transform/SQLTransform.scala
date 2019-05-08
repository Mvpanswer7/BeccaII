package xmatrix.dsl.transform

import org.apache.spark.sql._

trait SQLTransform {
    def transform(df: DataFrame, path: String, params: Map[String, String]): DataFrame
}
