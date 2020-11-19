import java.io.BufferedReader

import Utillities._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("Twitter-Feature-Extractor")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val input=setupInput()

    val twitterDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",input(0))
      .option("subscribe",input(1))
      .option("startingoffset","latest")
      .option("checkpointlocation","../checkpoint/test1/")
      .load()

    setupLogging()

    twitterDF
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }
}
