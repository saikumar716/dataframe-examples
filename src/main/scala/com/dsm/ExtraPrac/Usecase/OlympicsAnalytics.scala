package com.dsm.ExtraPrac.Usecase


import com.dsm.model.olympics
import com.dsm.utils.Constants
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OlympicsAnalytics {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("OlympicsAnalytics")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)
    val FilePath = "file:///D://BigData_DSM//WorkSpace//dataframe-examples//src//main//resources//data//olympix_data.csv"

    val textFile = sparkSession.sparkContext.textFile(FilePath)
                  .filter{rec => {if(rec.split("\t").length >= 10) true else false}}
                  .map(record => record.split("\t"))
                  .filter(x=>{if(x(5).equalsIgnoreCase("swimming")&& x(9).matches("\\d+")) true else false })
                  .map(a=> olympics(a(0),
                    a(1).toInt,
                    a(2),
                    a(3).toDouble,
                    a(4),
                    a(5),
                    a(6).toDouble,
                    a(7).toDouble,
                    a(8).toDouble,
                    a(9).toDouble
                  )).map(record => (record.Country,record.Total_Medals))
                    .sortBy(_._2, false)
                    .reduceByKey(_+_)

    textFile.foreach(println)

println(" Doing usecase1 it in Dataframes =====> ")
import sparkSession.implicits._
    textFile.toDF("Country", "TotalNoOfMedals")
      .groupBy($"Country")
      .agg(sum($"TotalNoOfMedals")).alias("TotalNoOfMedals")
      .orderBy(col("sum(TotalNoOfMedals)").desc)
      .show(5)

println("use case2====>")
    val textFile2 = sparkSession.sparkContext.textFile(FilePath)
      .filter{rec => {if(rec.split("\t").length >= 10) true else false}}
      .map(record => record.split("\t"))
      .filter(x=>{if(x(2).equalsIgnoreCase("India")&& x(9).matches("\\d+")) true else false })
      .map(a=> olympics(a(0),
        a(1).toInt,
        a(2),
        a(3).toDouble,
        a(4),
        a(5),
        a(6).toDouble,
        a(7).toDouble,
        a(8).toDouble,
        a(9).toDouble
      )).map(record => (record.Year,record.Total_Medals))
      .sortBy(_._2, false)
      .reduceByKey(_+_)

    textFile2.foreach(println)

    textFile2.toDF("year", "TotalNoOfMedals")
      .groupBy($"year")
      .agg(sum($"TotalNoOfMedals")).alias("TotalNoOfMedals")
      .orderBy(col("sum(TotalNoOfMedals)").desc)
      .show(5)


    println("use case3====>")
    val textFile3 = sparkSession.sparkContext.textFile(FilePath)
                    .filter { x => {if(x.toString().split("\t").length >= 10) true else false} }
                    .map(record => record.split("\t"))
                    .filter(x=>{if((x(9).matches(("\\d+")))) true else false })
                    .map(x => (x(2),x(9).toInt))
                    .sortBy(_._2, false)
                    .reduceByKey(_ + _)

    textFile3.foreach(println)
    textFile3.toDF("year", "TotalNoOfMedals")
      .groupBy($"year")
      .agg(sum($"TotalNoOfMedals"))
      .orderBy(col("sum(TotalNoOfMedals)").alias("TotalNoOfMedals").desc)
      .show(5)

  }

}
