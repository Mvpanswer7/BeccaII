package xmatrix.script

import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.Row


/**
  * Created by iodone on {18-7-2}.
  */
object ml  {

  def dataTest() = {
    val data = Seq(
      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0)))
    )

    val v0 = Vectors.sparse(4, Seq((0, 1.0), (3, -2.0)))
    println(v0)

  }

  def main(args: Array[String]): Unit = {
    dataTest()
  }

}
