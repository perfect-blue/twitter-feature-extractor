import java.util

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import Schema._
import org.apache.spark.sql.types.{DataType, DataTypes}
class FeatureExtractor(spark:SparkSession,dataframe:DataFrame) {
  import spark.implicits._

  val twitterBaseDF=dataframe
    .select(from_json(expr("CAST(value as string)"),GRAPH_PAYLOAD_STRUCT) as ("tweet"))

  val tweet=twitterBaseDF
    .select(
      $"tweet.payload.CreatedAt".as("Timestamp"),
      to_timestamp(from_unixtime(col("tweet.payload.CreatedAt").divide(1000))) as("CreatedAt"),
      $"tweet.payload.Id",
      $"tweet.payload.Text",
      $"tweet.payload.InReplyToStatusId",
      $"tweet.payload.InReplyToUserId",
      $"tweet.payload.InReplyToScreenName",
      $"tweet.payload.CurrentUserRetweetId",
      $"tweet.payload.RetweetCount".as("RetweetCount"),
      $"tweet.payload.FavoriteCount".as("FavoriteCount"),
      $"tweet.payload.Retweet",
      $"tweet.payload.User.Id".as("UserID"),
      $"tweet.payload.User.ScreenName".as("UserName"),
      $"tweet.payload.User.FollowersCount",
      $"tweet.payload.User.FriendsCount",
      $"tweet.payload.User.FavouritesCount",
      $"tweet.payload.User.StatusesCount",
      $"tweet.payload.User.Verified",
      $"tweet.payload.Lang",
      expr("tweet.payload.UserMentionEntities.Id") as("Mention_Id"),
      expr("tweet.payload.UserMentionEntities.Name") as("Mention_Name"),
      expr("tweet.payload.UserMentionEntities.Text") as("Mention_UserName")
    ).filter($"tweet.payload.Lang"==="in" || $"tweet.payload.Lang"==="en")

  def generateNodes():DataFrame={
    tweet
      .withColumn("Partition",getPartition($"CreatedAt"))
      .select(
        $"UserName",
        $"Text",
        $"FollowersCount",$"FriendsCount",$"FavouritesCount",$"StatusesCount",
        $"Verified",
        $"Partition"(2).as("Day"),
        $"Partition"(1).as("Month"),
        $"Partition"(0).as("Year")
      )
  }

  def generateEdges():DataFrame={
    val edges = tweet
      .withColumn("Interactions",
      getInteraction(col("InReplyToScreenName"),col("Retweet"), col("Mention_UserName")))
      .withColumn("Partition",getPartition($"CreatedAt"))
      .select(
        $"UserName".as("Source"),
        $"Interactions"(1).as("Target"),
        $"Partition"(2).as("Day"),
        $"Partition"(1).as("Month"),
        $"Partition"(0).as("Year")
      )

      edges
  }

  /**
   * For window functions, window start at Jan 1 1970, 0:00 (midnight) GMT
   * My computer is 18:00 GMT +7
   * @return
   */
  def generateWeightedEdges():DataFrame={
    val graphTweet=tweet
      .withColumn("Interactions",
        getInteraction(col("InReplyToScreenName"),col("Retweet"), col("Mention_UserName")))
      .select(
        $"CreatedAt",
        $"Id",
        $"Text",
        $"UserName".as("Source"),
        $"CurrentUserRetweetId",
        $"FollowersCount",
        $"FriendsCount",
        $"FavouritesCount",
        $"StatusesCount",
        $"Interactions"(0).as("Interaction"),
        $"Interactions"(1).as("Target"),
        $"Interactions"(2).cast(DataTypes.DoubleType).as("Interaction_weight"),
        $"Interactions"(3).cast(DataTypes.IntegerType).as("Interaction_count")
      )

    /**
     * 5 minutes watermark means:
     *  - a window will only be considered until the watermark surpases the window end
     *  - an element, row, record will be considered if after the watermark
     */
    val weighted_graph=graphTweet
      .withWatermark("CreatedAt","5 minutes")
      .groupBy(window(col("CreatedAt"), "5 minutes").as("Time"),
        col("Source"),
        col("Target"))
      .agg(
        count("Target").as("Count_Interaction"),
        avg("Interaction_weight").as("Avg_Interaction")
      )

    graphTweet.printSchema()
    weighted_graph.printSchema()
    weighted_graph
      .withColumn("Partition",getPartition($"Time.start"))
      .select(
        $"Source",
        $"Target",
        $"Count_Interaction",
        $"Avg_Interaction",
        $"Time.start",
        $"Time.end"
      )
   }

  /**
   * UDF
   */

  private val getInteraction = spark.udf.register("getInteraction",interaction)
  def interaction:(String,Boolean,Seq[String])=>
    Array[String]=(inReplyToScreenName:String,retweet:Boolean,mention:Seq[String])=>{
      val isTweet=inReplyToScreenName==null && retweet==false
      val isReply=inReplyToScreenName!=null && retweet==false
      val isRetweet=inReplyToScreenName==null && retweet==true

    if(isReply){
      Array("reply",inReplyToScreenName,"0.75","1")
    }else if(isRetweet){
      Array("retweet",mention(0),"0.5","1")
    }else{
      Array("tweet",null,"0.2","1")
    }
  }

  private val getPartition = spark.udf.register("getPartition",partitioning)
  def partitioning:String=>Array[String]=(column:String)=>{
      val dateTime=column.split("-")

      Array(dateTime(0),dateTime(1),dateTime(2).split(" ")(0))
  }
}
