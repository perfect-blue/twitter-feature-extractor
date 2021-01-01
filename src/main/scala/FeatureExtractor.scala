import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import Schema._


class FeatureExtractor(spark:SparkSession,dataframe:DataFrame) {
  import spark.implicits._

  val twitterBaseDF=dataframe
    .select(from_json(expr("CAST(value as string)"),GRAPH_PAYLOAD_STRUCT) as ("tweet"))

  val tweet=twitterBaseDF
    .withColumn("QuotedDF",
      when($"tweet.payload.QuoteStatus".isNotNull,
        from_json(expr("tweet.payload.QuoteStatus"),QUOTED_STATUS_STRUCT))
        .otherwise(null))
    .withColumn("RetweetDF",
      when($"tweet.payload.Retweet"===true,
        from_json(expr("tweet.payload.RetweetedStatus"),QUOTED_STATUS_STRUCT)).otherwise(null))

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
      $"tweet.payload.User.Protected",
      $"tweet.payload.Retweet",
      when($"RetweetDF".isNotNull,true).otherwise(false).as("IsRetweet"),
      when($"QuotedDF".isNotNull,true).otherwise(false).as("Quote"),
      $"tweet.payload.Lang",
      $"QuotedDF",
      $"RetweetDF"
    ).filter($"tweet.payload.Lang"==="in")


  def generateNodes():DataFrame={
    val flat_tweet=tweet.withColumn("RetweetObject", when($"RetweetDF".isNotNull,getObject(
      to_timestamp(from_unixtime(col("RetweetDF.CreatedAt").divide(1000))),
      $"RetweetDF.Id",
      $"RetweetDF.Text",
      $"RetweetDF.InReplyToScreenName",
      $"RetweetDF.RetweetCount",
      $"RetweetDF.FavoriteCount",
      $"RetweetDF.Retweet",
      $"RetweetDF.lang",
      $"RetweetDF.User.ScreenName",
      $"RetweetDF.User.FollowersCount",
      $"RetweetDF.User.FriendsCount",
      $"RetweetDF.User.FavouritesCount",
      $"RetweetDF.User.StatusesCount",
      $"RetweetDF.User.Verified")).otherwise(null))
      .withColumn("QuoteObject", when($"QuotedDF".isNotNull,
        getObject(
          to_timestamp(from_unixtime(col("QuotedDF.CreatedAt").divide(1000))),
          $"QuotedDF.Id",
          $"QuotedDF.Text",
          $"QuotedDF.InReplyToScreenName",
          $"QuotedDF.RetweetCount",
          $"QuotedDF.FavoriteCount",
          $"QuotedDF.Retweet",
          $"QuotedDF.lang",
          $"QuotedDF.User.ScreenName",
          $"QuotedDF.User.FollowersCount",
          $"QuotedDF.User.FriendsCount",
          $"QuotedDF.User.FavouritesCount",
          $"QuotedDF.User.StatusesCount",
          $"QuotedDF.User.Verified")).otherwise(null))
    .withColumn("Interaction",getInteraction($"InReplyToScreenName",$"Retweet",$"Quote"))
      .withColumn("Partition",getPartition($"CreatedAt"))
    .select(
      $"CreatedAt",
      $"TweetId",
      $"Text",
      $"ScreenName",
      $"FollowersCount",
      $"FriendsCount",
      $"FavouritesCount",
      $"StatusesCount",
      $"Verified",
      $"Lang",
      expr("CAST(RetweetObject as string)"),
      expr("CAST(QuoteObject as string)"),
      $"Partition"(0).as("Year"),
      $"Partition"(1).as("Month"),
      $"Partition"(2).as("Day")
    )

    val nodes = flat_tweet

    nodes
  }

  def generateEdges():DataFrame = {
      val result = tweet
        .withColumn("Interaction",getInteraction($"InReplyToScreenName",$"Retweet",$"Quote"))
        .withColumn("Mention",getMention($"Text"))
        .withColumn("Partition",getPartition($"CreatedAt"))
        .select(
          $"ScreenName".as("Source"),
            when($"Interaction"==="quote",$"QuotedDF.User.ScreenName")
              .when($"Interaction"==="reply",$"InReplyToScreenName")
              .when($"Interaction"==="retweet" && $"IsRetweet"===true,$"RetweetDF.User.ScreenName")
              .when($"Interaction"==="retweet" && $"IsRetweet"===false,$"Mention").otherwise(null).as("Target"),
          $"Interaction",
          $"Partition"(0).as("Year"),
          $"Partition"(1).as("Month"),
          $"Partition"(2).as("Day")
        ).filter($"Interaction"=!="tweet")
      result
  }


  /**
   * UDF
   */

  private val getInteraction = spark.udf.register("getInteraction",interaction)
  def interaction:(String,Boolean,Boolean)=>
    String=(inReplyToScreenName:String,isRetweet:Boolean,isQuote:Boolean)=> {

    val retweet = isRetweet==true
    val quote = inReplyToScreenName==null && isRetweet==false && isQuote==true
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

  private val getMention = spark.udf.register("getMention",mention)
  def mention:(String)=>String=(text:String)=>{
      val user = text.split(":")(0)
      val mention = user.split("@")(1)

      mention
  }

  private val getObject = spark.udf.register("getObject",textObject)
  def textObject:(String,String,String,String,Int,Int,Boolean,String,String,Int,Int,Int,Int,Boolean)
    =>Array[String]=(createdAt:String,tweetId:String,text:String,inReplyUsername:String,retweet:Int,favourite:Int,
                     isRetweet:Boolean,lang:String,username:String,followerCounts:Int,friendsCount:Int,favouriteCounts:Int,
                     statusesCount:Int,isVerified:Boolean)=>{

    Array(
      createdAt,
      tweetId,
      text,
      inReplyUsername,
      retweet.toString,
      favourite.toString,
      isRetweet.toString(),
      lang,
      username,
      followerCounts.toString,
      friendsCount.toString,
      favouriteCounts.toString,
      statusesCount.toString,
      isVerified.toString)
  }

  private val getPartition = spark.udf.register("getPartition",partitioning)
  def partitioning:String=>Array[String]=(column:String)=>{
      val dateTime=column.split("-")

      Array(dateTime(0),dateTime(1),dateTime(2).split(" ")(0))
  }
}
