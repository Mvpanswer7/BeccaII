package xmatrix.core.main

import xmatrix.core.XmatrixApp

/**
  * Created by iodone on {18-5-10}.
  */

object LocalSparkApp1 {
  /*
  mvn package -Ponline -Pcarbondata -Pbuild-distr -Phive-thrift-server -Pspark-1.6.1
   */
  def main(args: Array[String]): Unit = {
    XmatrixApp.main(Array(
      "-xmatrix.master", "local[2]",
      "-xmatrix.name", "Monster",
      "-xmatrix.rest", "true",
      "-xmatrix.platform", "spark",
      "-xmatrix.type", "stream",
      "-xmatrix.rest.ip", "0.0.0.0",
      "-xmatrix.rest.port", "9004",
      "-xmatrix.tispark", "true",
      "-xmatrix.rest.ip", "0.0.0.0",
      "-xmatrix.rest.port", "8124",
      "-xmatrix.metrics.kafka", "192.168.48.193:9092",
      "-spark.io.compression.codec", "lzf",
      "-spark.sql.streaming.fileSink.log.compactInterval", "10000000",
      //"-xmatrix.udfClassPath", "antiy.udf.sparksql.UDF",
      // "-xmatrix.enableHiveSupport", "true",
      "-xmatrix.spark.service", "true",
//      "-xmatrix.xql", "classpath:///xql/ss.xql",
      "-spark.tispark.pd.addresses", "192.168.200.191:2379"
    ))
  }
}
