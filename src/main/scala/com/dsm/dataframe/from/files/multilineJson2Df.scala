package com.dsm.dataframe.from.files

import org.apache.spark.sql.SparkSession
import com.dsm.utils.Constants

object multilineJson2Df {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.master("local[2]").appName("Multi Line Json To DF").getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)


    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", Constants.ACCESS_KEY)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", Constants.SECRET_ACCESS_KEY)

    import sparkSession.implicits._
    val path = "s3n://" + Constants.S3_BUCKET + "/json/employees_multiLine.json"

    val multiLineDf = sparkSession.read
     // .option("multiline", "true")
     // .option("mode","PERMISSIVE")
      .json(sparkSession.sparkContext.wholeTextFiles(path).values)
    multiLineDf.printSchema()
    multiLineDf.show(3)
    multiLineDf.select("empno", "designation").show()



    val  singleLineDf = sparkSession.read
      .json("s3n://" + Constants.S3_BUCKET + "/json/employees_singleLine.json")
    singleLineDf.printSchema()
    singleLineDf.show()
      singleLineDf.select("deptno").show()

    multiLineDf.except(singleLineDf).show()

    sparkSession.close()
  }
}
