package com.dsm.test

import org.apache.spark.sql.{SaveMode, SparkSession}
import com.typesafe.config.ConfigFactory
object Test {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("Spark Test").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    val events = List(
      "Prime Sound,10000",
      "Prime Sound,5000",
      "Sportsorg,20000",
      "Sportsorg,5000",
      "Ultra Sound,30000",
      "Ultra Sound,5000"
    )

    val eventsRDD = sparkSession.sparkContext.parallelize(events)
    eventsRDD.map(event => event.split(","))
      .map(event => (event(0), (event(1).toDouble)))
      .reduceByKey((budget1, budget2) => if(budget1 < budget2) budget1 else budget2)
      .foreach(println)

    val rootconfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3_config = rootconfig.getConfig("s3_conf")


    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3_config.getString("access_key"))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3_config.getString("secret_access_key"))

    val rdd = sparkSession.sparkContext.textFile(path = s"s3n://${s3_config.getString("s3_bucket")}/KC_Extract_1_20171009.csv")
    //    rdd.map(x => x.split(",")).map(r => (r(0),r(1))).reduceByKey((x,y) => (x+y)).take(3).foreach(println)

    rdd.take(3).foreach(println)
    sparkSession.close()
  }
}
