package com.dsm.ExtraPrac

import com.dsm.utils.Constants
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, lit, sum, when}
import org.apache.spark.sql.types.{DateType, DoubleType, StructType, TimestampType}

object PartialPivotDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.master("local[2]").appName("Dataframe Example").getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)
    import sparkSession.implicits._

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", Constants.ACCESS_KEY)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", Constants.SECRET_ACCESS_KEY)

    println("\nReading txn data,")
    val tranSchema = new StructType()
      .add("txn_ts", TimestampType, true)
      .add("amount", DoubleType, true)

    var tranDf = sparkSession.read
      .option("header", "false")
      .option("delimiter", ",")
      .option("nullValue", "NULL")
      .option("timestampFormat", "dd-MM-yyyy HH:mm:ss")
      .format("csv")
      .schema(tranSchema)
      .load("s3n://" + Constants.S3_BUCKET + "/failed_txn.csv")

    tranDf.printSchema()
    tranDf.show(false)

    println("\nAdding failed txn flag,")
    tranDf = tranDf
      .withColumn("txn_ts", tranDf("txn_ts").cast(DateType))
      .withColumn("failedTxnFlag", when($"amount".isNull, 1).otherwise(0))

    tranDf.printSchema()
    tranDf.show()

    println("\nCounting daily failed txns,")
    tranDf
      .groupBy($"txn_ts")
      .agg(sum(coalesce($"amount", lit(0))).as("total_txn"), sum($"failedTxnFlag").as("failed_txn_cnt"))
      .orderBy($"txn_ts")
      .show()


    sparkSession.close()
  }
}
