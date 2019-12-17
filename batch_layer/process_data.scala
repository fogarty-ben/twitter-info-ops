import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SaveMode
import org.apache.spark.ml.feature.{RegexTokenizer, NGram, StopWordsRemover}

spark.sql("add jar hdfs:///benfogarty/finalProject/serialize_users-0.0.1-SNAPSHOT.jar")

val tweets = spark.table("benfogarty_tweets")
val users = spark.table("benfogarty_users")

// Making a table counting English tweets with text/body by month
val tweetsText = tweets.select(date_format(tweets("tweet_time"), "MM-YYYY").as("tweet_month"), 
                               lower(tweets("tweet_text")).as("raw_tweet_text"),
                               tweets("userid"),
                               tweets("hashtags"),
                               tweets("quote_count"),
                               tweets("reply_count"),
                               tweets("like_count"),
                               tweets("retweet_count")).
                        filter(tweets("tweet_language")==="en").
                        filter(tweets("tweet_text").isNotNull).
                        filter(tweets("tweet_time").isNotNull)

// Making a table counting 2-, 3-, 4-, and 5-grams in English tweets with text/body by month
val urlPattern = "((\\w+:\\/\\/)[-a-zA-Z0-9:@;?&=\\/%\\+\\.\\*!'\\(\\),\\$_\\{\\}\\^~\\[\\]`#|]+)"
val rtPattern = "rt @[^:]+: "
val handlePattern = "@[^ ]+"
val hashtagPattern = "#\\w+"
val regexMatch = ("(" + urlPattern + ")|(" + rtPattern + ")|(" + 
                  handlePattern + ")|(" + hashtagPattern + ")")

val tweetsCleanText = tweetsText.select(tweetsText("tweet_month"),
                                        regexp_replace(tweetsText("raw_tweet_text"), 
                                                       regexMatch, "").as("raw_tweet_text"),
                                        tweetsText("userid"),
                                        tweetsText("quote_count"),
                                        tweetsText("reply_count"),
                                        tweetsText("like_count"),
                                        tweetsText("retweet_count"))

val regexTokenizer = new RegexTokenizer().
    setInputCol("raw_tweet_text").
    setOutputCol("raw_tweet_words").
    setPattern("[\\W-']")

val tweetsRawWords = regexTokenizer.transform(tweetsCleanText)

var stopWordsRemover = new StopWordsRemover().
                          setStopWords(StopWordsRemover.loadDefaultStopWords("english")).
                          setInputCol("raw_tweet_words").
                          setOutputCol("processed_tweet_words")

val tweetsProcessedWords = stopWordsRemover.transform(tweetsRawWords)

spark.sql("DROP TABLE IF EXISTS benfogarty_ngrams_by_month")
spark.sql("DROP TABLE IF EXISTS benfogarty_ngrams_by_month_top_users")
val nGramEngagementWindow = Window.partitionBy($"ngram").
                                  orderBy(desc("total_engagement_count"))
for (n <- 2 to 5) {

    val ngramTransformer = new NGram().setN(n).
                           setInputCol("processed_tweet_words").
                           setOutputCol("tweet_text_ngrams")

    val tweetsNGrams = ngramTransformer.transform(tweetsProcessedWords)

    val ngrams = tweetsNGrams.select(tweetsNGrams("tweet_month"),
                                    explode(tweetsNGrams("tweet_text_ngrams")).as("ngram"),
                                    tweetsNGrams("userid"),
                                    tweetsNGrams("quote_count"),
                                    tweetsNGrams("reply_count"),
                                    tweetsNGrams("like_count"),
                                    tweetsNGrams("retweet_count"))
    
    val nGramsByMonth = ngrams.groupBy(ngrams("tweet_month"), ngrams("ngram")).
                               agg(count("*").as("count"),
                                   sum(ngrams("quote_count")).as("total_quote_count"),
                                   sum(ngrams("reply_count")).as("total_reply_count"),
                                   sum(ngrams("like_count")).as("total_like_count"),
                                   sum(ngrams("retweet_count")).as("total_retweet_count"))

    nGramsByMonth.write.mode(SaveMode.Append).saveAsTable("benfogarty_ngrams_by_month")

    val nGramsByUser = ngrams.groupBy(ngrams("ngram"), ngrams("userid")).
                                     agg(count("*").as("count"),
                                         sum(ngrams("quote_count") + ngrams("reply_count") +
                                             ngrams("like_count") +  ngrams("retweet_count")).
                                          as("total_engagement_count"),
                                         sum(ngrams("quote_count")).as("total_quote_count"),
                                         sum(ngrams("reply_count")).as("total_reply_count"),
                                         sum(ngrams("like_count")).as("total_like_count"),
                                         sum(ngrams("retweet_count")).as("total_retweet_count"))

    val topUsersByNGram = nGramsByUser.withColumn("rank", row_number.over(nGramEngagementWindow)).
                                                    filter($"rank" <= 5)

    val topUsersByNGramInfo = topUsersByNGram.join(users, topUsersByNGram("userid") <=> users("user_id")).
                                                select(topUsersByNGram("ngram"),
                                                       users("user_display_name"),
                                                       users("user_screen_name"),
                                                       users("user_reported_location"),
                                                       users("user_profile_description"),
                                                       topUsersByNGram("count"),
                                                       topUsersByNGram("total_engagement_count"),
                                                       topUsersByNGram("total_quote_count"),
                                                       topUsersByNGram("total_reply_count"),
                                                       topUsersByNGram("total_like_count"),
                                                       topUsersByNGram("total_retweet_count"))
                                                   
    val topUsersByNGramAgg = topUsersByNGramInfo.groupBy(topUsersByNGramInfo("ngram")).
                                                  agg(concat_ws("\t,\t", collect_list(
                                                        topUsersByNGramInfo("user_display_name"))).
                                                     as("user_display_names"),
                                                      concat_ws("\t,\t", collect_list(
                                                        topUsersByNGramInfo("user_screen_name"))).
                                                      as("user_screen_names"),
                                                      concat_ws("\t,\t", collect_list(
                                                        topUsersByNGramInfo("user_reported_location"))).
                                                      as("user_reported_locations"),
                                                      concat_ws("\t,\t", collect_list(
                                                        topUsersByNGramInfo("user_profile_description"))).
                                                      as("user_profile_descriptions"),
                                                      concat_ws("\t,\t", collect_list(
                                                        topUsersByNGramInfo("total_engagement_count"))).
                                                      as("total_engagement_counts"),
                                                      concat_ws("\t,\t", collect_list(
                                                        topUsersByNGramInfo("total_quote_count"))).
                                                      as("total_quote_counts"),
                                                      concat_ws("\t,\t", collect_list(
                                                        topUsersByNGramInfo("total_reply_count"))).
                                                      as("total_reply_counts"),
                                                      concat_ws("\t,\t", collect_list(
                                                        topUsersByNGramInfo("total_like_count"))).
                                                      as("total_like_counts"),
                                                      concat_ws("\t,\t", collect_list(
                                                        topUsersByNGramInfo("total_retweet_count"))).
                                                      as("total_retweet_counts"))

    topUsersByNGramAgg.write.mode(SaveMode.Append).saveAsTable("benfogarty_ngrams_top_users")

}

// Making a table counting hashtags in English tweets with text/body by month
val tweetsHashtags = tweetsText.select(tweetsText("tweet_month"), 
                                   explode(tweetsText("hashtags")).as("hashtag"),
                                   tweetsText("userid"),
                                   tweetsText("quote_count"),
                                   tweetsText("reply_count"),
                                   tweetsText("like_count"),
                                   tweetsText("retweet_count"))

val tweetsProcessedHashtags = tweetsHashtags.select(tweetsHashtags("tweet_month"),
                                                    lower(tweetsHashtags("hashtag")).as("hashtag"),
                                                    tweetsHashtags("userid"),
                                                    tweetsHashtags("quote_count"),
                                                    tweetsHashtags("reply_count"),
                                                    tweetsHashtags("like_count"),
                                                    tweetsHashtags("retweet_count"))

val hashtagsByMonth = tweetsProcessedHashtags.groupBy(tweetsProcessedHashtags("tweet_month"),
                                                      tweetsProcessedHashtags("hashtag")).
                                              agg(count("*").as("count"),
                                                  sum(tweetsProcessedHashtags("quote_count")).
                                                  as("total_quote_count"),
                                                  sum(tweetsProcessedHashtags("reply_count")).
                                                  as("total_reply_count"),
                                                  sum(tweetsProcessedHashtags("like_count")).
                                                  as("total_like_count"),
                                                  sum(tweetsProcessedHashtags("retweet_count")).
                                                  as("total_retweet_count"))

hashtagsByMonth.write.mode(SaveMode.Overwrite).saveAsTable("benfogarty_hashtags_by_month")

val hashtagsByUser = tweetsProcessedHashtags.groupBy(tweetsProcessedHashtags("hashtag"),
                                                            tweetsProcessedHashtags("userid")).
                                                    agg(count("*").as("count"),
                                                        sum(tweetsProcessedHashtags("quote_count") + 
                                                            tweetsProcessedHashtags("reply_count") +
                                                            tweetsProcessedHashtags("like_count") +
                                                            tweetsProcessedHashtags("retweet_count")).
                                                        as("total_engagement_count"),
                                                        sum(tweetsProcessedHashtags("quote_count")).
                                                        as("total_quote_count"),
                                                        sum(tweetsProcessedHashtags("reply_count")).
                                                        as("total_reply_count"),
                                                        sum(tweetsProcessedHashtags("like_count")).
                                                        as("total_like_count"),
                                                        sum(tweetsProcessedHashtags("retweet_count")).
                                                        as("total_retweet_count"))

val hashtagEngagementWindow = Window.partitionBy($"hashtag").
                                     orderBy(desc("total_engagement_count"))

val topUsersByHashtag = hashtagsByUser.withColumn("rank", row_number().over(hashtagEngagementWindow)).
                                                    filter($"rank" <= 5)

val topUsersByHashtagInfo = topUsersByHashtag.join(users, topUsersByHashtag("userid") <=> users("user_id")).
                                              select(topUsersByHashtag("hashtag"),
                                                     users("user_display_name"),
                                                     users("user_screen_name"),
                                                     users("user_reported_location"),
                                                     users("user_profile_description"),
                                                     topUsersByHashtag("count"),
                                                     topUsersByHashtag("total_engagement_count"),
                                                     topUsersByHashtag("total_quote_count"),
                                                     topUsersByHashtag("total_reply_count"),
                                                     topUsersByHashtag("total_like_count"),
                                                     topUsersByHashtag("total_retweet_count"))

val topUsersByHashtagAgg =  topUsersByHashtagInfo.groupBy(topUsersByHashtagInfo("hashtag")).
                                                  agg(concat_ws("\t,\t", collect_list(
                                                    topUsersByHashtagInfo("user_display_name"))).
                                                     as("user_display_names"),
                                                      concat_ws("\t,\t", collect_list(
                                                        topUsersByHashtagInfo("user_screen_name"))).
                                                      as("user_screen_names"),
                                                      concat_ws("\t,\t", collect_list(
                                                        topUsersByHashtagInfo("user_reported_location"))).
                                                      as("user_reported_locations"),
                                                      concat_ws("\t,\t", collect_list(
                                                        topUsersByHashtagInfo("user_profile_description"))).
                                                      as("user_profile_descriptions"),
                                                      concat_ws("\t,\t", collect_list(
                                                        topUsersByHashtagInfo("total_engagement_count"))).
                                                      as("total_engagement_counts"),
                                                      concat_ws("\t,\t", collect_list(
                                                        topUsersByHashtagInfo("total_quote_count"))).
                                                      as("total_quote_counts"),
                                                      concat_ws("\t,\t", collect_list(
                                                        topUsersByHashtagInfo("total_reply_count"))).
                                                      as("total_reply_counts"),
                                                      concat_ws("\t,\t", collect_list(
                                                        topUsersByHashtagInfo("total_like_count"))).
                                                      as("total_like_counts"),
                                                      concat_ws("\t,\t", collect_list(
                                                        topUsersByHashtagInfo("total_retweet_count"))).
                                                      as("total_retweet_counts"))

topUsersByHashtagAgg.write.mode(SaveMode.Overwrite).saveAsTable("benfogarty_hashtags_top_users")

System.exit(0)
