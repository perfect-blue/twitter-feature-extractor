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
    ).filter($"tweet.payload.Lang"==="in")

  def graphFeature():DataFrame={
    val graphTweet=tweet
      .withColumn("Interactions",
        getInteraction(col("InReplyToScreenName"),col("Retweet"),
          col("Mention_UserName")))
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
        $"Interactions"(2).cast(DataTypes.IntegerType).as("Interaction_weight")
      )


    val weighted_graph=graphTweet
      .groupBy(window(col("CreatedAt"),"1 hours").as("Time"),
        col("Source"),
        col("Target"))
      .agg(
        sum("Interaction_weight").as("Count_Interaction"),
        avg("Interaction_weight").as("Avg_Interaction")
      )

    weighted_graph.sort($"Count_Interaction".desc)
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
      Array("reply",inReplyToScreenName,"3")
    }else if(isRetweet){
      Array("retweet",mention(0),"1")
    }else{
      Array("tweet",null,"0")
    }
  }

}
