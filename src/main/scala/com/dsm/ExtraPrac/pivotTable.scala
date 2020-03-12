package com.dsm.ExtraPrac

import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types._

object pivotTable {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("Dataframe Example")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)
    import sparkSession.implicits._

    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))

    val pivotPath1 = s"s3n://${s3Config.getString("s3_bucket")}/pivot1.csv"
    val pivotPath2 = s"s3n://${s3Config.getString("s3_bucket")}/pivot2.csv"

    val tranSchema1 = new StructType()
      .add("Product", IntegerType ,true)
      .add("Month", DoubleType ,true)
      .add("Sale",IntegerType,true)


    val pivot1DF = sparkSession.read
        .option("Header", "True")
        .schema(tranSchema1)
        .csv(pivotPath1)

    pivot1DF.printSchema()
    pivot1DF.show()

    //pivot1DF.withColumn("Month", to_date($"Month")).show()

    pivot1DF.groupBy($"Product")
      .pivot("Month")
      .sum("Sale")
      .orderBy("Product")
      .show()

    println("succeeded trans dataframe")

    val tranSchema = new StructType()
        .add("Transactions", TimestampType,true)
        .add("values", DoubleType,true)

    val pivot2DF = sparkSession.read
      .option("Header", "True")
      .option("nullValue", "NULL")
      .option("timestampFormat", "dd-MM-yyyy HH:mm:ss")
      .schema(tranSchema)
      .csv(pivotPath2)

    pivot2DF.printSchema()
    pivot2DF.show()

    pivot2DF.withColumn("Transactions", $"Transactions".cast(DateType)).groupBy("Transactions")
            .agg(sum("Values").as("succeeded trans"))
            .show()
  }
}
