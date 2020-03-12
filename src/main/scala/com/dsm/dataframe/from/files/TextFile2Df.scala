package com.dsm.dataframe.from.files

import com.dsm.utils.Constants
import org.apache.spark.sql.{SaveMode, SparkSession, types}
import org.apache.spark.sql.types._

object TextFile2Df {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.master("local[*]").appName("Dataframe Example").getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", Constants.ACCESS_KEY)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", Constants.SECRET_ACCESS_KEY)

    println("\nCreating dataframe from CSV file using 'SparkSession.read.format()',")
    val finSchema = new StructType()
      .add("id", IntegerType,true)
      .add("has_debt", BooleanType,true)
      .add("has_financial_dependents", BooleanType,true)
      .add("has_student_loans", BooleanType,true)
      .add("income", DoubleType,true)
    val finSchema2 = StructType(
      StructField("Id",IntegerType,true)::
      StructField("has_debt",BooleanType,true) ::
      StructField("has_financial_dependancy", BooleanType,true)::
      StructField("has_student_loans", BooleanType,true)::
      StructField("income",DoubleType,true)   :: Nil
    )

    val finDf = sparkSession.read
        .option("header", "false")
        .option("delimiter", ",")
        .format("csv")
        .schema(finSchema2)
        .load("s3n://" + Constants.S3_BUCKET + "/finances.csv")

    finDf.printSchema()
    finDf.show()

    println("Creating dataframe from CSV file using 'SparkSession.read.csv()',")
    val financeDf = sparkSession.read
      .option("mode", "DROPMALFORMED")
      .option("header", "false")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv("s3n://" + Constants.S3_BUCKET + "/finances.csv")
      .toDF("id", "has_debt", "has_financial_dependents", "has_student_loans", "income")

    println("# of partitions = " + finDf.rdd.getNumPartitions)
    financeDf.printSchema()
    financeDf.show()

    financeDf
      .repartition(2)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("delimiter", "~")
      .csv("s3n://" + Constants.S3_BUCKET + "/fin")

    println("reading from s3 bucket after writing with schema")
    val finDf2 = sparkSession.read
        .format("csv")
        .option("Header",true)
        .option("Delimiter","~")
        .schema(finSchema2)
        .load("s3n://" + Constants.S3_BUCKET + "/fin")
    finDf2.printSchema()
    finDf2.show()

    sparkSession.close()
  }
}
