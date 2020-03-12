package com.dsm.ExtraPrac
import java.io.File
import org.apache.spark.sql.{Row,SaveMode,SparkSession}


object SparkHiveIntegration {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val sparkSession = SparkSession.builder.master("local[*]").appName("Dataframe Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    import sparkSession.sql
    import sparkSession.implicits._
    val  Filepath =  "file:///D://BigData_DSM//WorkSpace//dataframe-examples//src//main//resources//data//person.csv"
    sql("CREATE TABLE IF NOT EXISTS person(firstName String, lastName String, age Int, weightInLbs DOUBLE, jobType String USING hive")
    sql("LOAD DATA LOAL INPATH 'firstName: String, lastName: String, age: Int, weightInLbs: Option[Double], jobType: Option[String]' INTO TABLE person")

    sql("SELECT * FROM person").show()
    sql("SELECT count(*) FROM person").show()

    var bankdf = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(Filepath)
    bankdf.show()
  }

}
