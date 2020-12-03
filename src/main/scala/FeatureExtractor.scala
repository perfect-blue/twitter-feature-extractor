import java.util

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import Schema._
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}
class FeatureExtractor(spark:SparkSession,dataframe:DataFrame) {
  import spark.implicits._

  val twitterBaseDF=dataframe
    .select(from_json(expr("CAST(value as string)"),GRAPH_PAYLOAD_STRUCT) as ("tweet"))

  val tweet=twitterBaseDF
    .withColumn("QuotedDF",
      when($"tweet.payload.QuoteStatus".isNotNull,
        from_json(expr("CAST(tweet.payload.QuoteStatus as string)"),QUOTED_STATUS_STRUCT))
        .otherwise(null))
    .withColumn("RetweetDF",
      when($"tweet.payload.RetweetedStatus".isNotNull,
        from_json(expr("CAST(tweet.payload.RetweetedStatus as string)"),QUOTED_STATUS_STRUCT)))
    .select(
      to_timestamp(from_unixtime(col("tweet.payload.CreatedAt").divide(1000))) as("CreatedAt"),
      $"tweet.payload.Id".as("TweetId"),
      $"tweet.payload.Text",
      $"tweet.payload.InReplyToScreenName",
      $"tweet.payload.RetweetCount",
      $"tweet.payload.FavoriteCount",
      $"tweet.payload.User.ScreenName",
      $"tweet.payload.User.FollowersCount",
      $"tweet.payload.User.FriendsCount",
      $"tweet.payload.User.FavouritesCount",
      $"tweet.payload.User.StatusesCount",
      $"tweet.payload.User.Verified",
      $"tweet.payload.Retweet",
      when($"QuotedDF".isNotNull,true).otherwise(false).as("Quote"),
      $"tweet.payload.Lang",
      when($"RetweetDF".isNotNull,$"RetweetDF.Text").otherwise(null).as("TargetRetweetText"),
      when($"RetweetDF".isNotNull,$"RetweetDF.RetweetCount").otherwise(null).as("TargetRetweetCount"),
      when($"QuotedDF".isNotNull,$"QuotedDF.Text").otherwise(null).as("TargetQuoteText"),
      when($"QuotedDF".isNotNull,$"QuotedDF.RetweetCount").otherwise(null).as("TargetQuoteCount"),
      when($"RetweetDF".isNotNull && $"QuotedDF".isNotNull,$"QuotedDF.User")
        .when($"RetweetDF".isNull && $"QuotedDF".isNotNull,$"QuotedDF.User")
        .when($"RetweetDF".isNotNull && $"QuotedDF".isNull,$"RetweetDF.User")
        .otherwise(null).as("TargetUser")
    ).filter($"tweet.payload.Lang"==="in" || $"tweet.payload.Lang"==="en")

  val flat_tweet=tweet
    .withColumn("Interaction",getInteraction($"InReplyToScreenName",$"Retweet",$"Quote"))
    .select(
      $"CreatedAt".as("SourceCreatedAt"),
      $"TweetId".as("SourceTweetId"),
      $"Text".as("SourceText"),
      $"ScreenName".as("Source"),
      $"InReplyToScreenName",
      $"FollowersCount".as("SourceFollowers"),
      $"FriendsCount".as("SourceFriends"),
      $"FavouritesCount".as("SourceFavourites"),
      $"StatusesCount".as("SourceStatuses"),
      $"Verified".as("SourceVerified"),
      $"Interaction",
      $"Lang",
      when($"Interaction"==="retweet",$"TargetRetweetText")
        .when($"Interaction"==="quote",$"TargetQuoteText")
        .otherwise(null).as("TargetText"),
      when($"Interaction"==="retweet",$"TargetRetweetCount")
        .when($"Interaction"==="quote",$"TargetQuoteCount")
        .otherwise(null).as("TargetRetweetCount"),
      $"TargetUser.ScreenName".as("TargetUsername"),
      $"TargetUser.FollowersCount".as("TargetFollowers"),
      $"TargetUser.FriendsCount".as("TargetFriends"),
      $"TargetUser.FavouritesCount".as("TargetFavourites"),
      $"TargetUser.StatusesCount".as("TargetStatuses"),
      $"TargetUser.Verified".as("TargetVerified")
    )


  def generateNodes():DataFrame={
    flat_tweet
  }

//  def generateEdges():DataFrame={
//    val edges = tweet
//      .withColumn("Interactions",
//      getInteraction(col("InReplyToScreenName"),col("Retweet"), col("Mention_UserName")))
//      .withColumn("Partition",getPartition($"CreatedAt"))
//      .select(
//        $"UserName".as("Source"),
//        $"Interactions"(1).as("Target"),
//        $"Partition"(2).as("Day"),
//        $"Partition"(1).as("Month"),
//        $"Partition"(0).as("Year")
//      )
//
//      edges
//  }

  /**
   * For window functions, window start at Jan 1 1970, 0:00 (midnight) GMT
   * My computer is 18:00 GMT +7
   * @return
   */
  def generateWeightedEdges(windows:String, watermarks:String):DataFrame={
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
      ).where("Target IS NOT NULL")

    /**
     * 5 minutes watermark means:
     *  - a window will only be considered until the watermark surpases the window end
     *  - an element, row, record will be considered if after the watermark
     */
    val weighted_graph=graphTweet
      .withWatermark("CreatedAt",windows)
      .groupBy(window(col("CreatedAt"), watermarks).as("Time"),
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
  def interaction:(String,Boolean,Boolean)=>
    String=(inReplyToScreenName:String,isRetweet:Boolean,isQuote:Boolean)=> {

    val retweet=isRetweet==true
    val quote=inReplyToScreenName==null && isRetweet==false && isQuote==true
    val reply=inReplyToScreenName!=null

    if (reply) {
      "reply"
    }else if(quote) {
      "quote"
    }else if(retweet){
      "retweet"
    }else{
      "tweet"
    }
  }
//  def interaction:(String,Boolean,Seq[String])=>
//    Array[String]=(inReplyToScreenName:String,retweet:Boolean,mention:Seq[String])=>{
//      val isTweet=inReplyToScreenName==null && retweet==false
//      val isReply=inReplyToScreenName!=null && retweet==false
//      val isRetweet=inReplyToScreenName==null && retweet==true
//
//    if(isReply){
//      Array("reply",inReplyToScreenName,"0.75","1")
//    }else if(isRetweet){
//      Array("retweet",mention(0),"0.5","1")
//    }else{
//      Array("tweet",null,"0.2","1")
//    }
//  }

  private val getPartition = spark.udf.register("getPartition",partitioning)
  def partitioning:String=>Array[String]=(column:String)=>{
      val dateTime=column.split("-")

      Array(dateTime(0),dateTime(1),dateTime(2).split(" ")(0))
  }
}
