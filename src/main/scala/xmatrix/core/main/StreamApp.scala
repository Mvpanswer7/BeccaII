package xmatrix.core.main

import xmatrix.core.XmatrixApp

/**
  * Created by iodone on {18-5-10}.
  */

object StreamApp {
  def main(args: Array[String]): Unit = {
    XmatrixApp.main(Array(
      "-xmatrix.master", "local[1]",
      "-xmatrix.name", "streamApp",
      "-xmatrix.rest", "true",
      "-xmatrix.platform", "spark",
      "-xmatrix.tispark", "false",
      // "-xmatrix.enableHiveSupport", "true",
      "-xmatrix.spark.service", "true",
      "-xmatrix.rest.ip", "0.0.0.0",
      "-xmatrix.rest.port", "8124",
      "-xmatrix.type", "stream"
    ))
  }
}
