import Utillities._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.concurrent.duration._

object Main {
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Twitter-Feature-Extractor")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val input=setupInput()

    val twitterDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",input(0))
      .option("subscribe",input(1))
      .load()

    setupLogging()
    val featureExtractor:FeatureExtractor=new FeatureExtractor(spark,twitterDF)
    val graphEdges = featureExtractor.generateEdges()
    val graphNodes = featureExtractor.generateNodes()
    val weighted_graph=featureExtractor.generateWeightedEdges()

   val a = weighted_graph
      .writeStream
      .format("console")
      .trigger(
        Trigger.ProcessingTime(5.minutes)
      )
      .outputMode("append")
      .option("truncate",false)
//      .option("header",true)
//      .option("path","D:/dump/graph-tweet/result")
//      .option("checkpointLocation","D:/dump/graph-tweet/checkpoint")
      .start()

    val b = weighted_graph
      .writeStream
      .format("parquet")
      .trigger(
        Trigger.ProcessingTime(5.minutes)
      )
      .outputMode("append")
      .option("truncate",false)
      .option("header",true)
      .option("path","D:/dump/graph-tweet/result")
      .option("checkpointLocation","D:/dump/graph-tweet/checkpoint")
      .start()

    a.awaitTermination()
    b.awaitTermination()
  }
}
