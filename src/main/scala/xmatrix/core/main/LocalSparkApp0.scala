package xmatrix.core.main

import xmatrix.core.XmatrixApp

/**
  * Created by iodone on {18-5-10}.
  */

object LocalSparkApp0 {
  def main(args: Array[String]): Unit = {
    XmatrixApp.main(Array(
      "-xmatrix.master", "local[1]",
      "-xmatrix.name", "Monster",
      "-xmatrix.rest", "true",
      "-xmatrix.platform", "spark",
      "-xmatrix.tispark", "true",
      "-xmatrix.type", "script",
       "-xmatrix.enableHiveSupport", "true",
      //"-xmatrix.enableCarbonDataSupport", "false",
      //"-xmatrix.carbondata.store", "/home/work/data/carbondata/store",
      //"-xmatrix.carbondata.meta", "/home/work/data/carbondata",
      "-xmatrix.spark.service", "true",
      "-xmatrix.rest.ip", "0.0.0.0",
      "-xmatrix.rest.port", "8124",
      //"-spark.tispark.pd.addresses", "192.168.200.:2379",
      "-spark.tispark.pd.addresses", "192.168.8.16:9101",
      "-spark.sql.extensions", "org.apache.spark.sql.TiExtensions"
    ))
  }
}
