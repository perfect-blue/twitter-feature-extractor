import org.apache.spark.sql.types.{DataTypes, LongType, StructType}

object Schema {

  /**
   * ########################################
   * GRAPH SCHEMA               #############
   * #######################################
   */

  /**
   * USER:
   * SCREEN_NAME_FIELD
   * FOLLOWERS_COUNT_FIELD
   * FRIENDS_COUNT_FIELD
   * STATUS_COUNT_FIELD
   */
  val USER_ID_FIELD="Id"
  val USER_NAME_FIELD="Name"
  val USER_SCREEN_NAME_FIELD="ScreenName"
  val USER_FOLLOWERS_COUNT_FIELD="FollowersCount"
  val USER_FRIENDS_COUNT_FIELD="FriendsCount"
  val USER_FAVORITE_COUNT_FIELD="FqvouritesCount"
  val USER_STATUS_COUNT_FIELD="StatusesCount"
  val USER_IS_VERIFIED_FIELD="Verified"
  val USER_IS_PROTECTED_FIELD="Protected"

  //Construct User Schema
  val USER_STRUCT = new StructType()
    .add(USER_ID_FIELD,DataTypes.StringType)
    .add(USER_NAME_FIELD,DataTypes.StringType)
    .add(USER_SCREEN_NAME_FIELD,DataTypes.StringType)
    .add(USER_FOLLOWERS_COUNT_FIELD, DataTypes.IntegerType)
    .add(USER_FRIENDS_COUNT_FIELD, DataTypes.IntegerType)
    .add(USER_FAVORITE_COUNT_FIELD, DataTypes.IntegerType)
    .add(USER_STATUS_COUNT_FIELD, DataTypes.IntegerType)
    .add(USER_IS_VERIFIED_FIELD, DataTypes.IntegerType)
    .add(USER_IS_PROTECTED_FIELD, DataTypes.IntegerType)

  /**
   * MENTIONS
   * USER_MENTION_FIELD
   *
   */
  val USER_MENTION_ID_FIELD="Id"
  val USER_MENTION_NAME_FIELD="Name"
  val USER_MENTION_TEXT_FIELD="Text"
  val USER_MENTION_USERNAME_FIELD="ScreenName"

  val USER_MENTION_ENTITY=new StructType()
    .add(USER_MENTION_ID_FIELD,DataTypes.StringType)
    .add(USER_MENTION_NAME_FIELD,DataTypes.StringType)
    .add(USER_MENTION_TEXT_FIELD,DataTypes.StringType)
    .add(USER_MENTION_USERNAME_FIELD,DataTypes.StringType)

  /**
   * STATUS:
   * STATUS_ID: Returns the ID of status
   * IN_REPLY_TO_STATUS_ID: Returns the status_ID of replied user
   * IN_REPLY_TO_USER_ID: Returns the user_ID of replied user
   * IN_REPLY_TO_USER_NAME: Returns the USER_NAME of replied user
   * FAVORITED_FIELD: Test if the status is favorited
   * RETWEETED_FIELD: Test if the status is retweeted
   * RETWEET_COUNT_FIELD: How many times this tweet has been retweeted by twitter user
   * FAVORITE_COUNT_FIELD: How many times this tweet has been favorited by twitter user
   * CURRENT_USER_RETWEET_ID: Returns the authenticating user's retweet's id
   *
   */
  val STATUS_ID_FIELD = "Id"
  val STATUS_TEXT_FIELD = "Text"
  val IN_REPLY_TO_STATUS_ID_FIELD="InReplyToStatusId"
  val IN_REPLY_TO_USER_ID_FIELD="InReplyToUserId"
  val IN_REPLY_TO_USER_NAME_FIELD="InReplyToScreenName"
  val USER_MENTION_FIELD="UserMentionEntity"
  val USER_FIELD="User"
  val FAVORITED_FIELD="Favorited"
  val RETWEETED_FIELD="Retweeted"
  val RETWEET_FIELD="Retweet"
  val RETWEET_COUNT_FIELD="RetweetCount"
  val FAVORITE_COUNT_FIELD="FavoriteCount"
  val CURRENT_USER_RETWEET_ID_FIELD="CurrentUserRetweetId"
  val LANG_FIELD="Lang"

  val STATUS_STRUCT:StructType = new StructType()
    .add(STATUS_ID_FIELD, DataTypes.StringType)
    .add(STATUS_TEXT_FIELD, DataTypes.StringType)
    .add(IN_REPLY_TO_STATUS_ID_FIELD, DataTypes.IntegerType)
    .add(IN_REPLY_TO_USER_ID_FIELD, DataTypes.IntegerType)
    .add(IN_REPLY_TO_USER_NAME_FIELD, DataTypes.IntegerType)
    .add(FAVORITED_FIELD, DataTypes.BooleanType)
    .add(RETWEETED_FIELD, DataTypes.BooleanType)
    .add(FAVORITE_COUNT_FIELD, DataTypes.IntegerType)
    .add(USER_FIELD, USER_FIELD)
    .add(RETWEET_FIELD,DataTypes.BooleanType)
    .add(USER_MENTION_FIELD,DataTypes.createArrayType(USER_MENTION_ENTITY))
    .add(RETWEET_COUNT_FIELD, DataTypes.IntegerType)
    .add(CURRENT_USER_RETWEET_ID_FIELD, DataTypes.IntegerType)
    .add(LANG_FIELD, DataTypes.StringType)

  val GRAPH_PAYLOAD_STRUCT:StructType= new StructType()
    .add("payload",STATUS_STRUCT)

}
