package com.dsm.dataframe.from.files

import com.dsm.utils.Constants
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object  nestedJson2Df {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.master("local[2]").appName("Multi Line Json To DF").getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)


    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", Constants.ACCESS_KEY)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", Constants.SECRET_ACCESS_KEY)

    import sparkSession.implicits._
    val path = "src/main/resources/data/nested.json"

    val nestedJsonDf = sparkSession.read
      // .option("multiline", "true")
      // .option("mode","PERMISSIVE")
      .json(sparkSession.sparkContext.wholeTextFiles(path).values)
    nestedJsonDf.printSchema()
    nestedJsonDf.show(3)

//    nestedJsonDf.select("end","start", "tags.name","tags.results","tags.stats.rawCount")
//      .toDF("end","start", "name","results","rawcount").show()

    val jsondata = nestedJsonDf
      .withColumn("end", $"end")
      .withColumn("start", $"start")
      .withColumn("Tagname", explode($"tags.name"))
      .withColumn("results", explode($"tags.results"))
      .withColumn("Site_IDraw", explode($"results.attributes.Site_ID"))
      .withColumn("Site_ID", explode($"Site_IDraw"))
      .withColumn("Tank_Noraw", explode($"results.attributes.Tank_No"))
      .withColumn("Tank_No", explode($"Tank_Noraw"))
      .withColumn("groups", explode($"results.groups"))
      .withColumn("groupName", explode($"groups.name"))
      .withColumn("type", explode($"groups.type"))
      .withColumn("values", explode($"results.values"))
      .withColumn("valueselarray", explode($"values"))
      .withColumn("rawCount", explode($"tags.stats.rawCount"))
      .withColumn("value", explode($"valueselarray"))
      .drop("tags")
      .drop("results")
      .drop("groups")
      .drop("values")
      .drop("valueselarray")
      .drop("Tank_Noraw")
      .drop("Site_IDraw")
    jsondata.show()

    jsondata.printSchema()



  }
}