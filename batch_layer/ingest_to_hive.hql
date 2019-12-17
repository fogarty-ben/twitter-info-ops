-- Map the Tweets CSV data in Hive
drop table if exists benfogarty_tweets_csv;

create external table benfogarty_tweets_csv(
  tweetid string,
  userid string,
  user_display_name string,
  user_screen_name string,
  user_reported_location string,
  user_profile_description string,
  user_profile_url string,
  follower_count int,
  following_count int,
  account_creation_date date,
  account_language string,
  tweet_language string,
  tweet_text string,
  tweet_time timestamp,
  tweet_client_name string,
  in_reply_to_tweetid string,
  in_reply_to_userid string,
  quoted_tweet_tweetid string,
  is_retweet boolean,
  retweet_userid string,
  retweet_tweetid string,
  latitude double,
  longitude double,
  quote_count int,
  reply_count int,
  like_count int,
  retweet_count int,
  hashtags array<string>,
  urls array<string>,
  user_mentions array<string>,
  poll_choices array<string>)
  row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)

STORED AS TEXTFILE
  location '/benfogarty/finalProject/tweets'

TBLPROPERTIES (
'skip.header.line.count'='1');

-- Create the ORC table
drop table if exists benfogarty_tweets;

create external table benfogarty_tweets(
  tweetid string,
  userid string,
  tweet_language string,
  tweet_text string,
  tweet_time timestamp,
  tweet_client_name string,
  in_reply_to_tweetid string,
  in_reply_to_userid string,
  quoted_tweet_tweetid string,
  is_retweet boolean,
  retweet_userid string,
  retweet_tweetid string,
  latitude double,
  longitude double,
  quote_count int,
  reply_count int,
  like_count int,
  retweet_count int,
  hashtags array<string>,
  urls array<string>,
  user_mentions array<string>,
  poll_choices array<string>)
  stored as orc;

-- Diable vectorization in execution engine for split function to work
set hive.vectorized.execution.enabled = false;

-- Move data from the CSV into the ORC table
insert overwrite table benfogarty_tweets(
  select CASE
      WHEN tweetid = '' THEN NULL
      WHEN TRUE THEN CAST(tweetid AS string)
    END,
    CASE
      WHEN userid = '' THEN NULL
      WHEN TRUE THEN CAST(userid AS string)
    END,
    CASE
      WHEN tweet_language = '' THEN NULL
      WHEN TRUE THEN CAST(tweet_language AS string)
    END,
    CASE
      WHEN tweet_text = '' THEN NULL
      WHEN TRUE THEN CAST(tweet_text AS string)
    END,
    CAST(tweet_time AS timestamp),
    CASE
      WHEN tweet_client_name = '' THEN NULL
      WHEN TRUE THEN CAST(tweet_client_name AS string)
    END,
    CASE
      WHEN in_reply_to_tweetid = '' THEN NULL
      WHEN TRUE THEN CAST(in_reply_to_tweetid AS string)
    END,
    CASE
      WHEN in_reply_to_userid = '' THEN NULL
      WHEN TRUE THEN CAST(in_reply_to_userid AS string)
    END,
    CASE
      WHEN in_reply_to_tweetid = '' THEN NULL
      WHEN TRUE THEN CAST(quoted_tweet_tweetid AS string)
    END,
    CAST(is_retweet AS boolean),
    CASE
      WHEN retweet_userid = '' THEN NULL
      WHEN TRUE THEN CAST(retweet_userid AS string)
    END,
    CASE
      WHEN retweet_tweetid = '' THEN NULL
      WHEN TRUE THEN CAST(retweet_tweetid AS string)
    END,
    CASE
      WHEN latitude = '' THEN NULL
      WHEN TRUE THEN CAST(latitude AS double)
    END,
    CASE
      WHEN longitude = '' THEN NULL
      WHEN TRUE THEN CAST(longitude AS double)
    END,
    CASE
      WHEN quote_count = '' THEN NULL
      WHEN TRUE THEN CAST(quote_count AS int)
    END,
    CASE
      WHEN reply_count = '' THEN NULL
      WHEN TRUE THEN CAST(reply_count AS int)
    END,
    CASE
      WHEN like_count = '' THEN NULL
      WHEN TRUE THEN CAST(like_count AS int)
    END,
    CASE
      WHEN retweet_count = '' THEN NULL
      WHEN TRUE THEN CAST(retweet_count AS int)
    END,
    CASE
      WHEN hashtags = '[]' OR hashtags = '' THEN NULL
      WHEN TRUE THEN SPLIT(REGEXP_REPLACE(hashtags, '[\\[\\]]', ''), ', ')
    END,
    CASE
      WHEN urls ='[]' OR urls = '' THEN NULL
      WHEN TRUE THEN SPLIT(REGEXP_REPLACE(urls, '[\\[\\]]', ''), ', ')
    END,
    CASE
      WHEN user_mentions ='[]' OR user_mentions = '' THEN NULL
      WHEN TRUE THEN SPLIT(REGEXP_REPLACE(user_mentions, '[\\[\\]]', ''), ', ')
    END,    
    CASE
      WHEN poll_choices = '[]' OR poll_choices = '' THEN NULL
      WHEN TRUE THEN SPLIT(REGEXP_REPLACE(poll_choices, '[\\[\\]]', ''), ', ')
    END
  from benfogarty_tweets_csv);

-- Re-enable vectorization
set hive.vectorized.execution.enabled = true;

-- Add serialize_users jar to class path
ADD JAR /home/benfogarty/finalProject/public/serialize_users-0.0.1-SNAPSHOT.jar;

-- Drop table if already existing
drop table if exists benfogarty_users;

CREATE EXTERNAL TABLE benfogarty_users
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer'
WITH SERDEPROPERTIES (
'serialization.class' = 'com.benfogarty.mpcs53014.User',
'serialization.format' = 'org.apache.thrift.protocol.TBinaryProtocol')
STORED AS SEQUENCEFILE
LOCATION '/benfogarty/finalProject/users';
