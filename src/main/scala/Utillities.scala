import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import scala.concurrent.duration._

//import {} from "../../../input.txt"
object Utillities {
  /**
   * konfigurasi logger sehingga hanya menampilkan pesan ERROR saja
   * untuk menghindari log spam
   */
  def setupLogging()={
    val logger = Logger.getRootLogger()
    logger.setLevel(Level.ERROR)
  }

  /**
   * Read input parameters
   */
  def setupInput(path:String):List[String] = {
    import scala.io.Source
    var parameters=List[String]()
    for (line <- Source.fromFile(path).getLines) {
     val fields = line.split("=")
     parameters :+=fields(1)

    }

    parameters
  }

  def saveToFiles(query: DataFrame, header:Boolean,format:String,path:String,trigger:String):StreamingQuery={
      query
        .writeStream
        .format(format)
        .trigger(
          Trigger.ProcessingTime(trigger.toInt.minutes)
        )
        .outputMode("append")
        .option("truncate","false")
        .option("header",header)
        .option("path",path)
        .partitionBy("Year","Month","Day")
        .option("checkpointLocation", path+"/checkpoint")
        .start()
  }

  def printConsole(query: DataFrame,trigger:String,mode:String):StreamingQuery={
      query
        .writeStream
        .option("truncate",false)
        .format("console")
        .trigger(
          Trigger.ProcessingTime(5.minutes)
        )
        .outputMode(mode)
        .start()
  }
}
