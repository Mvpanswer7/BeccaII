package xmatrix.script

/**
  * Created by iodone on {18-11-29}.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object spark extends App {

//  val store = "hdfs:///user/spark2/data/carbondata/store"
//  val meta = "hdfs:///user/spar2/data/carbondata"

  val store = "/home/work/data/carbondata/store"
  val meta = "/home/work/data/carbondata"


  import org.apache.spark.sql.CarbonSession._
  val carbon = SparkSession.builder().master("local").appName("CarbonSessionExample").getOrCreateCarbonSession(store, meta)

  import carbon._

  val df = carbon.read.option("header", true).csv("file:///home/work/app/gitavlyun/insight-xmatrix/src/main/resources/datasets/data2.csv")

//  df.createOrReplaceTempView("test_0")

//  carbon.sql("CREATE TABLE IF NOT EXISTS carbon_table_0(a0 string, b0 string, c0 string) STORED BY 'carbondata' ")
//  carbon.sql(s"Insert INTO TABLE carbon_table_0 select * from test_0")
  carbon.sql("drop table if exists carbon_table_10")

  df.write.format("carbondata").option("tableName", "carbon_table_10").option("compress", "true").mode(SaveMode.Overwrite).save()

  carbon.sql("SHOW SEGMENTS FOR TABLE carbon_table_10").show
  carbon.sql("desc formatted carbon_table_10").show
  carbon.sql("select * from carbon_table_10").show
  carbon.sql("SHOW SEGMENTS FOR TABLE carbon_table_10").show

}
