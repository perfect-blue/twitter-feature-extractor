import java.util

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import Schema._
class FeatureExtractor(spark:SparkSession,dataframe:DataFrame) {
  import spark.implicits._

  val twitterBaseDF=dataframe
    .select(from_json(expr("CAST(value as string)"),GRAPH_PAYLOAD_STRUCT) as ("tweet"))

  val tweet=twitterBaseDF
    .select(
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
      expr("tweet.payload.UserMentionEntities.Id") as("Mention_Id"),
      expr("tweet.payload.UserMentionEntities.Name") as("Mention_Name"),
      expr("tweet.payload.UserMentionEntities.Text") as("Mention_UserName")

    )

  def grapFeature():DataFrame={
    val graphTweet=tweet
      .withColumn("Interactions",
        getInteraction(col("InReplyToScreenName"),col("Retweet"),
          col("Mention_UserName")))
      .select($"Id",$"Text",$"UserName".as("source"),$"CurrentUserRetweetId",
        $"FollowersCount",$"FriendsCount",$"FavouritesCount",$"StatusesCount",
        $"Interactions"(0).as("Interaction"), $"Interactions"(1).as("target"))


    graphTweet

//      tweet.printSchema()
//      tweet
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
      Array("reply",inReplyToScreenName)
    }else if(isRetweet){
      Array("retweet",mention(0))
    }else{
      Array("tweet",null)
    }
  }


}
