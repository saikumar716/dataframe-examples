package com.dsm.ExtraPrac

import com.dsm.utils.Constants
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.types._

object Excelread {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("Dataframe Example")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)
    import sparkSession.implicits._

    println("\nCreating dataframe from xlsx file")
    val bankSchema = StructType(
      StructField("Account_No",StringType,true)::
        StructField("Date",TimestampType,true) ::
        StructField("Transaction_Details", StringType,true)::
        StructField("Cheque_No", LongType,true)::
        StructField("Value_Date",TimestampType,true)  ::
        StructField("Withdrawal_Amount",LongType,true) ::
        StructField("Deposit_Amount",LongType,true) ::
        StructField("Balance_Amount",LongType,true)   :: Nil
    )

   val  Filepath =  "file:///D://BigData_DSM//WorkSpace//dataframe-examples//src//main//resources//data//bank1.xlsx"

  var bankdf = sparkSession.sqlContext.read
    .format("com.crealytics.spark.excel")
    .option("dataAddress", "Sheet1") // Required
    .option("useHeader", "false") // Required
    .option("treatEmptyValuesAsNulls", "false") // Optional, default: true
    .option("inferSchema", "true") // Optional, default: false
    .option("addColorColumns", "true") // Optional, default: false
    .option("startColumn", 0) // Optional, default: 0
    .option("endColumn", 99) // Optional, default: Int.MaxValue
    .option("timestampFormat", "dd-MM-yyyy") // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
    //.option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files
    .option("excerptSize", 10) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
    .schema(bankSchema) // Optional, default: Either inferred schema, or all columns are Strings
    .load(Filepath)


    bankdf = bankdf.withColumn("Date", bankdf("Date").cast(DateType))
                   .withColumn("Value_Date", $"Value_Date".cast(DateType))
                   .withColumn("Account_No",regexp_replace(bankdf.col("Account_No"), "[^A-Z0-9_]", ""))

    bankdf = bankdf.withColumn("Account_No", bankdf.col("Account_No").cast(LongType))
    bankdf.cache()
    println("count: " + bankdf.count())
    bankdf.orderBy($"Balance_Amount").show()

  }
}
