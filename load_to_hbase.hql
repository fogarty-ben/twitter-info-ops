-- Move ngram_by_month to hbase
drop table if exists benfogarty_ngrams_by_month_hbase;

create external table benfogarty_ngrams_by_month_hbase (
  key string,
  tweet_month string, ngram string, count bigint,
  total_quote_count bigint, total_reply_count bigint,
  total_like_count bigint, total_retweet_count bigint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,ngrams:tweet_month,ngrams:ngram,ngrams:count,ngrams:total_quote_count,ngrams:total_reply_count,ngrams:total_like_count,ngrams:total_retweet_count')
TBLPROPERTIES ('hbase.table.name' = 'benfogarty_ngrams_by_month');

insert overwrite table benfogarty_ngrams_by_month_hbase
select concat(ngram, '_', tweet_month),
  tweet_month, ngram, count,
  total_quote_count, total_reply_count,
  total_like_count, total_retweet_count from benfogarty_ngrams_by_month;

-- Move hashtags_by_month to hbase
drop table if exists benfogarty_hashtags_by_month_hbase;

create external table benfogarty_hashtags_by_month_hbase (
  key string,
  tweet_month string, hashtag string, count bigint,
  total_quote_count bigint, total_reply_count bigint,
  total_like_count bigint, total_retweet_count bigint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,hashtags:tweet_month,hashtags:hashtag,hashtags:count,hashtags:total_quote_count,hashtags:total_reply_count,hashtags:total_like_count,hashtags:total_retweet_count')
TBLPROPERTIES ('hbase.table.name' = 'benfogarty_hashtags_by_month');

insert overwrite table benfogarty_hashtags_by_month_hbase
select concat(hashtag, '_', tweet_month),
  tweet_month, hashtag, count,
  total_quote_count, total_reply_count,
  total_like_count, total_retweet_count from benfogarty_hashtags_by_month;

-- Move ngrams_by_month_top_users to hbase
drop table if exists benfogarty_ngrams_top_users_hbase;

create external table benfogarty_ngrams_top_users_hbase (
  ngram string,
  user_display_names string,
  user_screen_names string,
  user_reported_locations string,
  user_profile_descriptions string,
  total_engagement_counts string,
  total_quote_counts string, total_reply_counts string,
  total_like_counts string, total_retweet_counts string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,ngrams:user_display_names,ngrams:user_screen_names,ngrams:user_reported_locations,ngrams:user_profile_descriptions,ngrams:total_engagement_counts,ngrams:total_quote_counts,ngrams:total_reply_counts,ngrams:total_like_counts,ngrams:total_retweet_counts')
TBLPROPERTIES ('hbase.table.name' = 'benfogarty_ngrams_top_users');

insert overwrite table benfogarty_ngrams_top_users_hbase
select ngram,
  user_display_names,
  user_screen_names,
  user_reported_locations,
  user_profile_descriptions,
  total_engagement_counts,
  total_quote_counts, total_reply_counts,
  total_like_counts, total_retweet_counts from benfogarty_ngrams_top_users;

-- Move hashtags_by_month_top_users to hbase
drop table if exists benfogarty_hashtags_top_users_hbase;

create external table benfogarty_hashtags_top_users_hbase (
  hashtag string,
  user_display_names string,
  user_screen_names string,
  user_reported_locations string,
  user_profile_descriptions string,
  total_engagement_counts string,
  total_quote_counts string, total_reply_counts string,
  total_like_counts string, total_retweet_counts string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,hashtags:user_display_names,hashtags:user_screen_names,hashtags:user_reported_locations,hashtags:user_profile_descriptions,hashtags:total_engagement_counts,hashtags:total_quote_counts,hashtags:total_reply_counts,hashtags:total_like_counts,hashtags:total_retweet_counts')
TBLPROPERTIES ('hbase.table.name' = 'benfogarty_hashtags_top_users');

insert overwrite table benfogarty_hashtags_top_users_hbase
select hashtag,
  user_display_names,
  user_screen_names,
  user_reported_locations,
  user_profile_descriptions,
  total_engagement_counts,
  total_quote_counts, total_reply_counts,
  total_like_counts, total_retweet_counts from benfogarty_hashtags_top_users;