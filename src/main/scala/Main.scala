import Utillities._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.concurrent.duration._
import Schema._

object Main {
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Twitter-Feature-Extractor")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val input=setupInput("input.txt")

    val twitterDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",input(0))
      .option("subscribe",input(1))
      .load()

    val path=input(2)
    val windows=input(3)
    val watermark=input(4)
    val trigger=input(5).split(" ")(0)
    val format=input(6)
    val keyword="/"+input(7)+"/"

    setupLogging()
    val featureExtractor:FeatureExtractor=new FeatureExtractor(spark,twitterDF)
    val graphEdges = featureExtractor.generateEdges()
    val graphNodes = featureExtractor.generateNodes()
//    val weightedGraphEdges=featureExtractor.generateWeightedEdges(windows, watermark)

    val graphEdgesQuery=saveToFiles(graphEdges,true,format,true,path+keyword+"edges",trigger)
    val graphNodesQuery=saveToFiles(graphNodes,true,format,true,path+keyword+"nodes",trigger)
//    val weightedEdgesQuery=saveToFiles(weightedGraphEdges,true,format,false,path +"weighted-edges",trigger)

    graphEdgesQuery.awaitTermination()
    graphNodesQuery.awaitTermination()
//    weightedEdgesQuery.awaitTermination()
  }
}
