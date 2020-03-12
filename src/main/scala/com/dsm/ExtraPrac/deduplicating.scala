package com.dsm.ExtraPrac

import com.dsm.utils.Constants
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_list, collect_set, struct}

object deduplicating {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("Dataframe Example")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)
    import sparkSession.implicits._
    val df = Seq(
      ("a", "b", 1),
      ("a", "b", 2),
      ("a", "b", 2),
      ("z", "b", 4),
      ("b", "a", 5),
      ("b", "a", 5),
      ("a", "x", 6)
    ).toDF("letter1", "letter2", "number1")

    df.show()

    df.dropDuplicates("letter1", "letter2", "number1").show()

    df
      .groupBy("letter1", "letter2")
      .agg(collect_list("number1") as "number1s")
      .show()
    df
      .groupBy("letter1", "letter2")
      .agg(collect_set("number1") as "number1s")
      .show()
    val playersDF = Seq(
      ("123", 11, "20180102", 0),
      ("123", 11, "20180102", 0),
      ("123", 13, "20180105", 3),
      ("555", 11, "20180214", 1),
      ("888", 22, "20180214", 2)
    ).toDF("player_id", "game_id", "game_date", "goals_scored")

    playersDF.show()
    playersDF
      .withColumn("as_struct", struct("game_id", "game_date", "goals_scored"))
      .groupBy("player_id")
      .agg(collect_set("as_struct") as "as_structs")
      .show(false)

  }
}
