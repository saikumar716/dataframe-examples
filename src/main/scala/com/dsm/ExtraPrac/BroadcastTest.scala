package com.dsm.ExtraPrac
import com.dsm.utils.Constants
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Usage: BroadcastTest [partitions] [numElem] [blockSize]
 */
object BroadcastTest {
  def main(args: Array[String]): Unit = {

    val blockSize = if (args.length > 2) args(2) else "4096"

    val sparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("Dataframe Example")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)

    val slices = if (args.length > 0) args(0).toInt else 2
    val num = if (args.length > 1) args(1).toInt else 1000000

    val arr1 = (0 until num).toArray

    for (i <- 0 until 3) {
      println(s"Iteration $i")
      println("===========")
      val startTime = System.nanoTime
      val barr1 = sparkSession.sparkContext.broadcast(arr1)
      val observedSizes = sparkSession.sparkContext.parallelize(1 to 10, slices).map(_ => barr1.value.length)
      // Collect the small RDD so we can print the observed sizes locally.
      observedSizes.collect().foreach(i => println(i))
      println("Iteration %d took %.0f milliseconds".format(i, (System.nanoTime - startTime) / 1E6))

      }

    sparkSession.stop()
  }
}
// scalastyle:on println