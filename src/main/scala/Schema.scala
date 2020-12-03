import org.apache.spark.sql.types.{DataTypes, LongType, StructField, StructType}

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
  val USER_FIELD="User"
  val USER_ID_FIELD="Id"
  val USER_NAME_FIELD="Name"
  val USER_SCREEN_NAME_FIELD="ScreenName"
  val USER_FOLLOWERS_COUNT_FIELD="FollowersCount"
  val USER_FRIENDS_COUNT_FIELD="FriendsCount"
  val USER_FAVORITE_COUNT_FIELD="FavouritesCount"
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
    .add(USER_IS_VERIFIED_FIELD, DataTypes.BooleanType)
    .add(USER_IS_PROTECTED_FIELD, DataTypes.BooleanType)

  /**
   * MENTIONS
   * USER_MENTION_FIELD
   *
   */
  val USER_MENTION_ENTITY_FIELD="UserMentionEntities"
  val USER_MENTION_ID_FIELD="Id"
  val USER_MENTION_NAME_FIELD="Name"
  val USER_MENTION_TEXT_FIELD="Text"
  val USER_MENTION_USERNAME_FIELD="ScreenName"

  val USER_MENTION_ENTITY_STRUCT=new StructType()
    .add(USER_MENTION_ID_FIELD,DataTypes.StringType)
    .add(USER_MENTION_NAME_FIELD,DataTypes.StringType)
    .add(USER_MENTION_TEXT_FIELD,DataTypes.StringType)
    .add(USER_MENTION_USERNAME_FIELD,DataTypes.StringType)

  /**
   * URL entities
   */
  val URL_ENTITY_FIELD="URLEntities"
  val URL_ENTITY_URL_FIELD="URL"
  val URL_ENTITY_TEXT_FIELD="Text"
  val URL_ENTITY_EXPANDED_URL_FIELD="ExpandedURL"
  val URL_ENTITY_START_FIELD="Start"
  val URL_ENTITY_END_FIELD="End"
  val URL_ENTITY_DISPLAY_URL_FIELD="DisplayURL"

  val URL_ENTITY_STRUCT=new StructType()
    .add(URL_ENTITY_URL_FIELD, DataTypes.StringType)
    .add(URL_ENTITY_TEXT_FIELD, DataTypes.StringType)
    .add(URL_ENTITY_EXPANDED_URL_FIELD, DataTypes.StringType)
    .add(URL_ENTITY_START_FIELD, DataTypes.IntegerType)
    .add(URL_ENTITY_END_FIELD, DataTypes.IntegerType)
    .add(URL_ENTITY_DISPLAY_URL_FIELD,DataTypes.StringType)

  /**
   * MEDIA ENTITY
   */

  val MEDIA_ENTITY_FIELD="MediaEntities"
  val MEDIA_ENTITY_ID_FIELD="Id"
  val MEDIA_ENTITY_TYPE_FIELD="Type"
  val MEDIA_ENTITY_MEDIA_URL_FIELD="MediaURL"
  val MEDIA_ENTITY_SIZE_FIELD="Sizes"
  val MEDIA_ENTITY_MEDIA_URL_HTTPS_FIELD="MediaURLHttps"
  val MEDIA_ENTITY_VIDEO_ASPECT_RATIO_WIDTH_FIELD="VideoAspectRatioWidth"
  val MEDIA_ENTITY_VIDEO_ASPECT_RATIO_HEIGHT_FIELD="VideoAspectRatioHeight"
  val MEDIA_ENTITY_VIDEO_DURATION_MILLIS_FIELD="VideoDurationMillis"
  val MEDIA_ENTITY_VIDEO_VARIANTS_FIELD="VideoVariants"
  val MEDIA_ENTITY_EXT_ALT_TEXT_FIELD="ExtAltText"
  val MEDIA_ENTITY_URL_FIELD="URL"
  val MEDIA_ENTITY_TEXT_FIELD="Text"
  val MEDIA_ENTITY_EXPANDED_URL_FIELD="ExpandedURL"
  val MEDIA_ENTITY_START_FIELD="Start"
  val MEDIA_ENTITY_END_FIELD="End"
  val MEDIA_ENTITY_DISPLAY_URL_FIELD="DisplayURL"


  /**
   * MEDIA ENTITY SIZES
   */
  val MEDIA_ENTITY_SIZE_RESIZE_FIELD="Resize"
  val MEDIA_ENTITY_SIZE_WIDTH_FIELD="Width"
  val MEDIA_ENTITY_SIZE_HEIGHT_FIELD="Height"

  val MEDIA_ENTITY_SIZES_STRUCT=new StructType()
    .add(MEDIA_ENTITY_SIZE_RESIZE_FIELD,DataTypes.IntegerType)
    .add(MEDIA_ENTITY_SIZE_WIDTH_FIELD,DataTypes.IntegerType)
    .add(MEDIA_ENTITY_SIZE_HEIGHT_FIELD,DataTypes.IntegerType)

  /**
   * MEDIA ENTITY VARIANTS
   */
  val MEDIA_ENTITY_VARIANTS_URL_FIELD="Url"
  val MEDIA_ENTITY_VARIANTS_BITRATE_FIELD="Bitrate"
  val MEDIA_ENTITY_VARIANTS_CONTENT_TYPE_FIELD="ContentType"

  val MEDIA_ENTITY_VARIANT_STRUCT=new StructType()
    .add(MEDIA_ENTITY_VARIANTS_URL_FIELD,DataTypes.StringType)
    .add(MEDIA_ENTITY_VARIANTS_BITRATE_FIELD, DataTypes.IntegerType)
    .add(MEDIA_ENTITY_VARIANTS_CONTENT_TYPE_FIELD,DataTypes.StringType)


  val MEDIA_ENTITY_STRUCT=new StructType()
    .add(MEDIA_ENTITY_ID_FIELD, DataTypes.StringType)
    .add(MEDIA_ENTITY_TYPE_FIELD,DataTypes.StringType)
    .add(MEDIA_ENTITY_MEDIA_URL_FIELD,DataTypes.StringType)
    .add(MEDIA_ENTITY_SIZE_FIELD,DataTypes.createMapType(DataTypes.IntegerType,MEDIA_ENTITY_SIZES_STRUCT))
    .add(MEDIA_ENTITY_MEDIA_URL_HTTPS_FIELD,DataTypes.StringType)
    .add(MEDIA_ENTITY_EXT_ALT_TEXT_FIELD,DataTypes.StringType)
    .add(MEDIA_ENTITY_VIDEO_ASPECT_RATIO_WIDTH_FIELD, DataTypes.IntegerType)
    .add(MEDIA_ENTITY_VIDEO_ASPECT_RATIO_HEIGHT_FIELD,DataTypes.IntegerType)
    .add(MEDIA_ENTITY_VIDEO_DURATION_MILLIS_FIELD,DataTypes.IntegerType)
    .add(MEDIA_ENTITY_VIDEO_VARIANTS_FIELD, MEDIA_ENTITY_VARIANT_STRUCT)
    .add(MEDIA_ENTITY_URL_FIELD, DataTypes.StringType)
    .add(MEDIA_ENTITY_TEXT_FIELD,DataTypes.StringType)
    .add(MEDIA_ENTITY_EXPANDED_URL_FIELD,DataTypes.StringType)
    .add(MEDIA_ENTITY_START_FIELD,DataTypes.IntegerType)
    .add(MEDIA_ENTITY_END_FIELD,DataTypes.IntegerType)
    .add(MEDIA_ENTITY_DISPLAY_URL_FIELD,DataTypes.StringType)

  /**
   * EXTENDED MEDIA ENTITY
   */
  val EXTENDED_MEDIA_ENTITY_FIELD="ExtendedMediaEntity"
  val EXTENDED_MEDIA_ID_FIELD="Id"
  val EXTENDED_MEDIA_TYPE_FIELD="Type"
  val EXTENDED_MEDIA_MEDIA_URL_FIELD="MediaURL"
  val EXTENDED_MEDIA_URL_FIELD = "URL"
  val EXTENDED_TEXT_URL_FIELD = "Text"
  val EXTENDED_EXPANDED_URL = "ExpandedURL"


  /**
   * QUOTED STATUS
   */
  val QUOTED_STATUS_FIELD="QuoteStatus"
  val RETWEET_STATUS_FIELD="RetweetedStatus"
  val QUOTED_CREATED_AT_FIELD="CreatedAt"
  val QUOTED_STATUS_ID_FIELD = "Id"
  val QUOTED_STATUS_TEXT_FIELD = "Text"
  val QUOTED_IN_REPLY_TO_STATUS_ID_FIELD="InReplyToStatusId"
  val QUOTED_IN_REPLY_TO_USER_ID_FIELD="InReplyToUserId"
  val QUOTED_IN_REPLY_TO_USER_NAME_FIELD="InReplyToScreenName"
  val QUOTED_FAVORITED_FIELD="Favorited"
  val QUOTED_RETWEETED_FIELD="Retweeted"
  val QUOTED_RETWEET_FIELD="Retweet"
  val QUOTED_RETWEET_COUNT_FIELD="RetweetCount"
  val QUOTED_FAVORITE_COUNT_FIELD="FavoriteCount"
  val QUOTED_CURRENT_USER_RETWEET_ID_FIELD="CurrentUserRetweetId"
  val QUOTED_CONTRIBUTOR_FIELD="Contributors"
  val QUOTED_LANG_FIELD="Lang"

  val QUOTED_STATUS_STRUCT:StructType = new StructType()
    .add(QUOTED_CREATED_AT_FIELD,DataTypes.StringType)
    .add(QUOTED_STATUS_ID_FIELD, DataTypes.StringType)
    .add(QUOTED_STATUS_TEXT_FIELD, DataTypes.StringType)
    .add(QUOTED_IN_REPLY_TO_STATUS_ID_FIELD, DataTypes.StringType)
    .add(QUOTED_IN_REPLY_TO_USER_ID_FIELD, DataTypes.StringType)
    .add(QUOTED_IN_REPLY_TO_USER_NAME_FIELD, DataTypes.StringType)
    .add(QUOTED_FAVORITED_FIELD, DataTypes.BooleanType)
    .add(QUOTED_RETWEETED_FIELD, DataTypes.BooleanType)
    .add(QUOTED_FAVORITE_COUNT_FIELD, DataTypes.IntegerType)
    .add(QUOTED_RETWEET_COUNT_FIELD, DataTypes.IntegerType)
    .add(USER_FIELD, USER_STRUCT)
    .add(QUOTED_RETWEET_FIELD,DataTypes.BooleanType)
    .add(USER_MENTION_ENTITY_FIELD,DataTypes.createArrayType(USER_MENTION_ENTITY_STRUCT))
    .add(QUOTED_CURRENT_USER_RETWEET_ID_FIELD, DataTypes.StringType)
    .add( QUOTED_CONTRIBUTOR_FIELD,DataTypes.createArrayType(DataTypes.StringType))
    .add(QUOTED_LANG_FIELD, DataTypes.StringType)
    .add(URL_ENTITY_FIELD,DataTypes.createArrayType(URL_ENTITY_STRUCT))
    .add(MEDIA_ENTITY_FIELD,DataTypes.createArrayType(MEDIA_ENTITY_STRUCT))


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
  val CREATED_AT_FIELD="CreatedAt"
  val STATUS_ID_FIELD = "Id"
  val STATUS_TEXT_FIELD = "Text"
  val IN_REPLY_TO_STATUS_ID_FIELD="InReplyToStatusId"
  val IN_REPLY_TO_USER_ID_FIELD="InReplyToUserId"
  val IN_REPLY_TO_USER_NAME_FIELD="InReplyToScreenName"
  val FAVORITED_FIELD="Favorited"
  val RETWEETED_FIELD="Retweeted"
  val RETWEET_FIELD="Retweet"
  val RETWEET_COUNT_FIELD="RetweetCount"
  val FAVORITE_COUNT_FIELD="FavoriteCount"
  val CURRENT_USER_RETWEET_ID_FIELD="CurrentUserRetweetId"
  val CONTRIBUTOR_FIELD="Contributors"
  val LANG_FIELD="Lang"

  val STATUS_STRUCT:StructType = new StructType()
    .add(CREATED_AT_FIELD,LongType,false)
    .add(STATUS_ID_FIELD, DataTypes.StringType,false)
    .add(STATUS_TEXT_FIELD, DataTypes.StringType,false)
    .add(IN_REPLY_TO_STATUS_ID_FIELD, DataTypes.StringType)
    .add(IN_REPLY_TO_USER_ID_FIELD, DataTypes.StringType)
    .add(IN_REPLY_TO_USER_NAME_FIELD, DataTypes.StringType)
    .add(FAVORITED_FIELD, DataTypes.BooleanType)
    .add(RETWEETED_FIELD, DataTypes.BooleanType)
    .add(FAVORITE_COUNT_FIELD, DataTypes.IntegerType)
    .add(RETWEET_COUNT_FIELD, DataTypes.IntegerType)
    .add(USER_FIELD, USER_STRUCT)
    .add(QUOTED_STATUS_FIELD,DataTypes.StringType)
    .add(RETWEET_STATUS_FIELD,DataTypes.StringType)
    .add(RETWEET_FIELD,DataTypes.BooleanType)
    .add(USER_MENTION_ENTITY_FIELD,DataTypes.createArrayType(USER_MENTION_ENTITY_STRUCT))
    .add(CURRENT_USER_RETWEET_ID_FIELD, DataTypes.StringType)
    .add(CONTRIBUTOR_FIELD,DataTypes.createArrayType(DataTypes.StringType))
    .add(LANG_FIELD, DataTypes.StringType)


  val GRAPH_PAYLOAD_STRUCT:StructType= new StructType()
    .add("payload",STATUS_STRUCT)



}
