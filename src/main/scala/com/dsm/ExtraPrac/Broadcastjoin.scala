package com.dsm.ExtraPrac

import com.dsm.utils.Constants
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object Broadcastjoin {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("Dataframe Example")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)
    import sparkSession.implicits._


    val  Filepath =  "file:///D://BigData_DSM//WorkSpace//dataframe-examples//src//main//resources//data//loan1.csv"

    var bankdf = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(Filepath)

    println("Big file ")
    bankdf.show(5)
    //bankdf.cache()
    //println("count: " + bankdf.count())
    val loanerDF = Seq(
      ("RENT", "3 years"),
      ("MORTGAGE", "5 years")
    ).toDF("home_ownership", "type")

    println("small  file ")
    loanerDF.show()
    println("joining both using broadcast join")
    bankdf.join(broadcast(loanerDF),bankdf("home_ownership") === loanerDF("home_ownership")).show()
    println("physical plan of  broadcast join")
    bankdf.join(broadcast(loanerDF),bankdf("home_ownership") === loanerDF("home_ownership")).explain()
    println("physical plan of  normal join")
    bankdf.join(loanerDF,bankdf("home_ownership") === loanerDF("home_ownership") ).explain()

    println("joining both using broadcast join deleting duplicates")
    bankdf.join(broadcast(loanerDF),Seq("home_ownership")).show()
    println("physical plan of  broadcast join")
    bankdf.join(broadcast(loanerDF),Seq("home_ownership")).explain()
    println("physical plan of  normal join")
    bankdf.join(loanerDF,Seq("home_ownership")).explain()


    println("joining both using broadcast join")
    bankdf.join(broadcast(loanerDF),bankdf("home_ownership") === loanerDF("home_ownership")).drop("home_ownership").show()
    bankdf.join(broadcast(loanerDF),bankdf("home_ownership") === loanerDF("home_ownership")).drop("home_ownership").explain()

    println("You can pass the explain() method a true argument to see" +
      " the parsed logical plan, analyzed logical plan, and optimized logical plan in addition to the physical plan.")
    bankdf.join(broadcast(loanerDF),bankdf("home_ownership") === loanerDF("home_ownership")).drop("term").explain(true)


    bankdf.join(loanerDF,bankdf("home_ownership") === loanerDF("home_ownership")).drop("home_ownership").explain(true)
    sparkSession.close()
  }
}
