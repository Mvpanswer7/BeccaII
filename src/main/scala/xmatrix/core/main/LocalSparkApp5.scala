package xmatrix.core.main

import xmatrix.core.XmatrixApp

/**
  * Created by iodone on {18-5-10}.
  */

object LocalSparkApp5 {
  /*
  mvn package -Ponline -Pcarbondata -Pbuild-distr -Phive-thrift-server -Pspark-1.6.1
   */
  def main(args: Array[String]): Unit = {
    XmatrixApp.main(Array(
      "-xmatrix.master", "local[2]",
      "-xmatrix.name", "Monster-ml",
      "-xmatrix.rest", "false",
      "-xmatrix.platform", "spark",
      "-xmatrix.type", "script",
      "-xmatrix.xql", "classpath:///xql/dmp.xql"
    ))
  }
}