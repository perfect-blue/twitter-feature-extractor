import Utillities._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.concurrent.duration._
import Schema._

object Main {
  val spark = SparkSession.builder()
    .appName("Twitter-Feature-Extractor")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val input=setupInput(args(0))

    val twitterDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",input(0))
      .option("subscribe",input(1))
      .load()

    val path=input(2)
    val trigger=input(3).split(" ")(0)
    val format=input(4)
    val keyword="/"+input(5)+"/"

    setupLogging()
    val featureExtractor:FeatureExtractor=new FeatureExtractor(spark,twitterDF)
    val graphEdges = featureExtractor.generateEdges()
    val graphNodes = featureExtractor.generateNodes()

    println(graphEdges.isStreaming)
    println(graphNodes.isStreaming)

    val graphEdgesQuery=saveToFiles(graphEdges,true,format,path+keyword+"edges",trigger)
    val graphNodesQuery=saveToFiles(graphNodes,true,format,path+keyword+"nodes",trigger)

    graphEdgesQuery.awaitTermination()
    graphNodesQuery.awaitTermination()

  }
}
