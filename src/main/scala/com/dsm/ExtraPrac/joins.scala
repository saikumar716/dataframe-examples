package com.dsm.ExtraPrac

import com.dsm.model.number
import com.dsm.utils.Constants
import org.apache.spark.sql.SparkSession

object joins {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("Dataframe Example")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)
    import sparkSession.implicits._

    val numDf = sparkSession.createDataFrame(List(
      number(1),
      number(1),
      number(1)
    ))
    val numDf1 = sparkSession.createDataFrame(List(
      number(1),
      number(1)
    ))
    numDf.show()
    numDf1.show()

    numDf.join(numDf1, Seq("id"), "inner").show(false) //6
    numDf.join(numDf1, Seq("id"), "outer").show(false) //6
    numDf.join(numDf1, Seq("id"), "left").show(false) //6
    numDf.join(numDf1, Seq("id"), "right").show(false) //6
    numDf.join(numDf1, Seq("id"), "left_semi").show(false) //3
    numDf.join(numDf1, Seq("id"), "left_anti").show(false) //0
  }
}
