package xmatrix.core.main

import xmatrix.core.XmatrixApp

/**
  * Created by iodone on {18-5-10}.
  */

object LocalSparkApp3 {
  /*
  mvn package -Ponline -Pcarbondata -Pbuild-distr -Phive-thrift-server -Pspark-1.6.1
   */
  def main(args: Array[String]): Unit = {
    XmatrixApp.main(Array(
      "-xmatrix.master", "local[2]",
      "-xmatrix.name", "Monster-ml",
      "-xmatrix.service", "false",
      "-xmatrix.rest", "false",
      "-xmatrix.platform", "spark",
      "-xmatrix.type", "script",
      "-xmatrix.xql", "classpath:///xql/word2vec.xql"
    ))
  }
}
