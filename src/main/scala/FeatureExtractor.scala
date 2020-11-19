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
      $"tweet.payload.Favorited",
      $"tweet.payload.Retweeted",
      $"tweet.payload.RetweetCount".as("Retweet"),
      $"tweet.payload.FavoriteCount".as("Favorite"),
      $"tweet.payload.Retweet",
      $"tweet.payload.User.Id".as("UserID"),
      $"tweet.payload.User.ScreenName".as("UserName"),
      $"tweet.payload.User.FollowersCount",
      $"tweet.payload.User.FriendsCount",
      $"tweet.payload.User.FavouritesCount",
      $"tweet.payload.User.StatusesCount",
      $"tweet.payload.User.Verified",
      expr("cast(tweet.payload.UserMentionEntities.Id as String)") as("Metion_Id"),
      expr("cast(tweet.payload.UserMentionEntities.Name as String)") as("Metion_Name"),
      expr("cast(tweet.payload.UserMentionEntities.Text as String)") as("Metion_UserName"),
      expr("cast(tweet.payload.UserMentionEntities.ScreenName as String)") as("Metion_Name")

    )

  def grapFeature():DataFrame={

    tweet
   }

  /**
   * UDF
   */

//  private val getInteraction = spark.udf.register("getInteraction",interaction)
//  def interaction:(String,Boolean,String,List[String])=>
//    String=(InReplyToScreenName:String,Retweet)=>{
//
//  }


}
