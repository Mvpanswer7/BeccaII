package xmatrix.core.main

import xmatrix.core.XmatrixApp

/**
  * Created by iodone on {18-5-10}.
  */

object LocalSparkApp4 {
  /*
  mvn package -Ponline -Pcarbondata -Pbuild-distr -Phive-thrift-server -Pspark-1.6.1
   */
  def main(args: Array[String]): Unit = {
    XmatrixApp.main(Array(
      "-xmatrix.master", "local[1]",
      "-xmatrix.name", "Monster",
      "-xmatrix.rest", "false",
      "-xmatrix.platform", "spark",
      "-xmatrix.tispark", "true",
      // "-xmatrix.enableHiveSupport", "true",
      "-xmatrix.spark.service", "false",
      "-xmatrix.unitest.awaitTermination","true",
      "-xmatrix.rest.ip", "0.0.0.0",
      "-xmatrix.rest.port", "8124",
      "-xmatrix.xql", "classpath:///xql/test-onetime.xql",
      "-spark.tispark.pd.addresses", "192.168.200.191:2379"
    ))
  }
}
