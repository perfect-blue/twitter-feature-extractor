import org.apache.log4j.{Level, Logger}
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
  def setupInput() = {
    import scala.io.Source

    for (line <- Source.fromFile("input.txt").getLines) {
//      val fields = line.split(" ")
//      if (fields.length == 2) {
//        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
//      }
      print(line+"\n")
    }
  }
}
