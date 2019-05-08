package test.xmatrix

import java.util.concurrent.TimeUnit

import org.antlr.v4.runtime.tree._
import org.antlr.v4.runtime.{ANTLRInputStream, CommonTokenStream, ParserRuleContext, TokenStreamRewriter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import xmatrix.common.XmatrixSqlParser
import xmatrix.core.XmatrixApp


/**
  * Created by iodone on {18-5-10}.
  */

object Test{
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .config("spark.io.compression.codec", "lzf")
      .master("local[*]")
      .appName("test")
      .getOrCreate()

    import spark.implicits._
    var df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.48.193:9092")
      .option("subscribe", "englog-origin")
      .load()

    df = df.withWatermark("timestamp", "1 hour")
      .groupBy(window($"timestamp", "10 seconds"), $"partition", $"", $"")
      .count()

    df.writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
      .start()
      .awaitTermination()
  }
}

object TestJoin{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .config("spark.io.compression.codec", "lzf")
      .master("local[*]")
      .appName("test")
      .getOrCreate()

    var df1 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.48.193:9092")
      .option("subscribe", "englog-origin")
      .load()
    df1 = df1.selectExpr("topic as topic1", "partition as partition1", "offset as offset1", "timestamp as timestamp1")
    df1 = df1.withWatermark("timestamp1", "1 hour")

    var df2 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.48.193:9092")
      .option("subscribe", "ss_test")
      .load()
    df2 = df2.selectExpr("topic as topic2", "partition as partition2", "offset as offset2", "timestamp as timestamp2")
    df2 = df2.withWatermark("timestamp2", "1 hour")

    val df3 = df1.join(df2, expr("""partition1 = partition2 and timestamp1 >= timestamp2 and timestamp1 <= timestamp2 + interval 1 minutes"""), "inner")
    df3.writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .outputMode("append")
      .start()
      .awaitTermination()
  }
}

object TestContinuous{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
        .config("spark.streaming.kafka.consumer.cache.enabled", false)
      .master("local[*]")
      .appName("test")
      .getOrCreate()
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.48.193:9092")
      .option("subscribe", "englog_origin")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("console")
      .trigger(Trigger.Continuous("10 seconds"))
      .start()
      .awaitTermination()
  }
}

object MultiSink{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .config("spark.io.compression.codec", "lzf")
      .master("local[*]")
      .appName("test")
      .getOrCreate()

    val df1 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.48.193:9092")
      .option("subscribe", "englog_origin")
      .load()
    val df2 = df1.selectExpr("topic as topic2", "partition as partition2", "offset as offset2", "timestamp as timestamp2")
    df1.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.48.193:9092")
      .option("topic", "test")
      .option("checkpointLocation", "C:\\Users\\yangyixin\\Desktop\\test\\ss_test\\checkpoint")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
    df2.writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
    spark.streams.awaitAnyTermination()
  }
}

import org.apache.spark.sql.TiContext

object TestTimestamp {
  def main(args: Array[String]) = {
    val ss = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.tispark.pd.addresses","192.168.8.16:9101")
      //.config("spark.tispark.request.timezone.offset", "-28800")
      .getOrCreate()
    val tiContext = new TiContext(ss)
    tiContext.conf.set("spark.tispark.request.timezone.offset", "-800")
    val table =  tiContext.tidbMapTable("insight_sources", "tidb_attack_info")
    val t1 = table.filter("uuid = '4c771124-d016-11e7-90f7-000c29a15974'")//.filter("created_at = '2017-11-23 14:22:07'")
    //created_at ="2017-11-23 14:22:07" and updated_at ="2017-11-23 14:22:07"
    t1.show()
    //table.show()
  }
}
object  TestTimezone {
  def main(args: Array[String]) = {
    Class.forName("com.mysql.jdbc.Driver")
    val ss = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()
    val jdbcDF = ss.read
      .format("jdbc")
      .option("url", "jdbc:mysql://192.168.8.16:4000/insight_sources?characterEncoding=utf8")
      .option("dbtable", "tidb_attack_info")
      .option("user", "insight-write")
      .option("password", "123")
      .load()
    val t1 = jdbcDF.filter("uuid = '4c771124-d016-11e7-90f7-000c29a15974'")
    t1.show
  }
}

object TestClickhouse {
  def main(args: Array[String]): Unit = {
    Class.forName("ru.yandex.clickhouse.ClickHouseDriver")
    val ss = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()
    val jdbcDF = ss.read
      .format("jdbc")
      .option("url", "jdbc:clickhouse://192.168.200.151:8123?connect_timeout=1000&receive_timeout=3000")
      .option("dbtable", "eds_app_detection_local")
      .option("user", "default")
      .option("password", "1Qaaz2Wsx")
      .load()
    import ss.implicits._
    //val df2 = jdbcDF.filter($"log_id" === "b37d63d3-4829-4609-b9eb-866e3b75b91a-b94097bde9")
    jdbcDF.show()
  }
}

object TestCkDataSource {
  import org.apache.spark.sql.execution.datasources.clickhouse._
  import org.apache.spark.sql.execution.datasources.clickhouse.spark._

  case class Row1(name: String, v: Int, v2: Int)
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.enableHiveSupport()
      .master("local[2]")
      .appName("hive-clickhouse-test")
      .getOrCreate()

    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    // test dframe
    val newdf = sqlContext.createDataFrame(1 to 10 map(i => Row1(s"$i", i, i + 10)) )
    //val newdf = sparkSession.sql("select * from hue_songbo_alluser_labels")
    val df = ClickhouseSparkExt.extraOperations(newdf)

    // clickhouse params
    val host = "192.168.200.151"
    val port = 8123
    val db = "default"
    val tableName = "jc_user_labels"
    //    val clusterName = None: Option[String]
    // start clickhouse docker using config.xml from clickhouse_files
    val clusterName = Some("clickhouse_log"): Option[String]

    // define clickhouse connection
    ClickhouseConnectionFactory.setUserName("default")
    ClickhouseConnectionFactory.setPassword("1Qaaz2Wsx")
    implicit val clickhouseDataSource = ClickhouseConnectionFactory.get(host, port)

    // create db / table
    //df.dropClickhouseDb(db, clusterName)
    //df.createClickhouseDb(db, clusterName)
    //df.createClickhouseTable(db, tableName, "dt", Seq( "dt", "id", "imei", "imei_md5", "sex", "borrow", "car", "digital_electronic", "finance", "game_box", "house", "network_novels", "porn", "real_estate_finance", "stocks", "vulgar"), clusterName)

    // save data
    //val res = df.saveToClickhouse("default", "jctest3", (row) => java.sql.Date.valueOf("1997-10-30"), "dt", clusterNameO = clusterName)
  }
}

object TestCkSs {
  import org.apache.spark.sql.streaming._

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("test")
      .getOrCreate()

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.48.193:9092")
      .option("subscribe", "englog_origin")
      .load()

    df.writeStream
      .outputMode("append")
      .format("console")
      //.option("implClass", "org.apache.spark.sql.execution.streaming.ClickHouseSinkProvider")
      .option("path", "jctest2")
      .option("checkpointLocation", "test")
      .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
      .start()
      .awaitTermination()

    //df.writeStream
    //  .outputMode("Append")
    //  .format("ClickHouseEventsSinkProvider")
    //  .option("duration", "5")
    //  .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
    //  .start()
      //.awaitTermination()

    // transform input data to stream of events

    // statefull transformation: word => Iterator[Event] => Iterator[StateUpdate]


    //query.awaitTermination()
  }

}

object TestHiveParser {
  def main(args: Array[String]): Unit = {
    val sql = "SELECT pageid, adid FROM pageAds LATERAL VIEW explode(adid_list) adTable AS adid"
    //import org.apache.spark.util.SQLAnalysis
    ////SQLAnalysis.run(sql)
    //val pd = new ParseDriver()
    //val tree = pd.parse(sql)
    //println(tree.toStringTree)
    //println(tree.dump())
    //SQLAnalysis.run(sql)
    ////val visitor =
    //println(SQLAnalysis.tableName)
  }
}

object TestHiveAntlr4 {
  def main(args: Array[String]): Unit = {
    val sql = "SELECT pageAds.pageid, pageAds.adid FROM pageAds as t1 LATERAL VIEW explode(adid_list) adTable AS adid"
    val sql2 = "select pageAds.* from pageAds group by pageAds.col3 order by pageAds.col2 as table2"
    val sql3 = "select pageAds.col1,pageAds.col2  from pageAds join tableB on pageAds.colA == tableB.colB "
    val sql4 = "select * from table1 where timstamp = 'aaa'"
    val sql5 = "select * from table1 where tabl1.timstamp = 'aaa'"
    //UnionAll 还有问题
    val sql6 = "select * from table1 union all select * from tabl2 "
    val sql7 = "select s1.created_at as created_at_1,s1.creation_date as creation_date_1,s1.dnssec as dnssec_1,s1.domain as domain_1,s1.expiration_date as expiration_date_1,s1.registrant_city as registrant_city_1,s1.registrant_email as registrant_email_1,s1.registrant_name as registrant_name_1,s1.registrant_phone as registrant_phone_1,s1.registrar as registrar_1,s1.registrar_id as registrar_id_1,s1.status as status_1,s1.update_date as update_date_1,s1.update_time as update_time_1,s1.updated_at as updated_at_1,s1.uuid as uuid_1 from tidb_whois_info as s1 limit 999 "
    val sql8 = "select a.* from ( select * from agg_lihua_ipxz_1210 where length(id)=16 and length(imei_p)>=14 and  length(imei_p)>=16 ) a left join agg_lihua_xz_user_1208 b on (a.id=b.id and a.imei_p=b.imei) where b.id is null and b.imei is null"
    val sql9 = "select hue_guochen_ceshi_2.ssid,hue_guochen_ceshi_2.wifi_mac,hue_guochen_lasted_result_04_10.score from  (select distinct ssid,wifi_mac from hue_guochen_ceshi_2)  left join hue_guochen_lasted_result_04_10   on hue_guochen_lasted_result_04_10.bssid=hue_guochen_ceshi_2.wifi_mac as result1;"
    val sql10 = "load csv.`/user/spark2/test1`options header=\"true\" as t1; select * from t1  union all select * from t1 union all select * from t1  as t2"

    //val sss = XmatrixSqlParser.replaceTableName(sql,  "_groupId")
    //println("$$$  " + XmatrixSqlParser.getTableName(sss))
    //println(sss)
    //println("--------")
    //val sss2 = XmatrixSqlParser.replaceTableName(sql2,  "_groupId")
    //println("$$$  " + XmatrixSqlParser.getTableName(sss2))
    //println(sss2)
    //println("--------")
    //val sss3 = XmatrixSqlParser.replaceTableName(sql3,  "_groupId")
    //println("$$$  " + XmatrixSqlParser.getTableName(sss3))
    //println(sss3)
    //println("--------")
    //val sss4 = XmatrixSqlParser.replaceTableName(sql4,  "_groupId")
    //println("$$$  " + XmatrixSqlParser.getTableName(sss4))
    //println(sss4)
    //println("--------")
    //val sss5 = XmatrixSqlParser.replaceTableName(sql5,  "_groupId")
    //println("$$$  " + XmatrixSqlParser.getTableName(sss5))
    //println(sss5)
    //println("--------")
    //val sss6 = XmatrixSqlParser.replaceTableName(sql6,  "_groupId")
    //println("$$$  " + XmatrixSqlParser.getTableName(sss6))
    //println(sss6)
    //println("--------")
    val sss7 = XmatrixSqlParser.replaceTableNames(sql7,  "_groupId")
    println("$$$  " + XmatrixSqlParser.getTableName(sss7))
    println(sss7)
  }
}

