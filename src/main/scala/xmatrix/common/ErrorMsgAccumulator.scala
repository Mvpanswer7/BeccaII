package xmatrix.common

import org.apache.spark.util.AccumulatorV2

import java.util.Collections

class ErrorMsgAccumulator extends AccumulatorV2[(String, String), java.util.Map[String, String]] {

  private val _map: java.util.Map[String, String] =
    new java.util.HashMap

  override lazy val value: java.util.Map[String, String] =
    Collections.synchronizedMap(_map)

  override def isZero: Boolean =
    _map.isEmpty

  override def copyAndReset(): ErrorMsgAccumulator =
    new ErrorMsgAccumulator()

  override def copy(): ErrorMsgAccumulator = {
    val newAcc = new ErrorMsgAccumulator
    _map.synchronized {
      newAcc._map.putAll(_map)
    }
    newAcc
  }

  override def reset(): Unit =
    _map.clear()

  override def add(v: (String, String)): Unit =
    _map.synchronized {
      unsafeAdd(v._1, v._2)
    }

  override def merge(other: AccumulatorV2[(String, String), java.util.Map[String, String]]): Unit =
    other match {
      case o: ErrorMsgAccumulator =>
        _map.synchronized {
          other.synchronized {
            import scala.collection.JavaConversions._
            o._map.foreach((unsafeAdd _).tupled)
          }
        }
      case _ => throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }

  private def unsafeAdd(k: String, v: String) = {
    _map.put(k, v)
  }

  def get(A: String) = {
    _map.getOrDefault(A, "")
  }
}