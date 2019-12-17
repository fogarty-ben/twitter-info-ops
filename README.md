# MPCS 53014 Final Project

Ben Fogarty
University of Chicago

12 December 2019

## Project scope

This project takes publicly available datasets on state-sponsored Twitter
information operations and makes the text of these tweets and the hashtags
within them searchable to identify trends in the tweet content over time and the
types of messengers used to discuss different topics. The most well-know example
of such an information operation was the Russian-backed Internet Research
Agency's network of Twitter accounts in use during and after the 2016 US
Presidential Election.

Data sets containing user account information, tweet information, and media for
multiple identified state-backed information operations are available
through the Twitter data archive at https://transparency.twitter.com/en/
information-operations.html. For this project, however, the batch layer is built
exclusively on data from the Internet Research Agency dataset released in
October 2018 and only uses the account information and tweet information
datasets. Though the tweets data set also contains all the accout information
data in each row, for the sake of demonstrating proficiency in joining datasets,
the project ingests the tweets data as if it does not contain account
information in each row beyond a unique userid field.

## File locations

All files used to create this project are located either on compute nodes of the
class cluster in the /home/benfogarty/finalProject/ directory or on the
webserver node of the class cluster in the /home/benfogarty/finalProject/
directory. 

## Screenshots

A screenshot of the app running are available in the submission directory in the
screenshots/ subdirectory. A video of the app running is available on Google
Drive via the link: https://drive.google.com/file/d/1ftFZL5WF1ikxxQLaZ3u9iMPuZLa4nmXk/view?usp=sharing.
The video was too large to upload via GradeScope.

## Running the app

To run the app, run the following command on a compute node of the class cluster
to start the speed layer:

    spark-submit --class StreamTweets /home/benfogarty/finalProject/uber-speed_layer_tweets-0.0.1-SNAPSHOT.jar mpcs53014c10-m-6-20191016152730.us-central1-a.c.mpcs53014-2019.internal:6667

And run the following commands on the webserver node of the class cluster to
start the web app:

    source /home/benfogarty/miniconda3/bin/activate
    python3 /home/benfogarty/finalProject/app.py

The web app should now be accessible via port 3043.

## Sample speed layer datasets

Sample datasets to test the speed layer can be obtained by downloading a set of
tweets from the Twitter data archive at https://transparency.twitter.com/
en/information-operations.html and submitting the dowloaded CSV through the web
app. As far as I have tested, the format for all tweet data dumps has been the
same, so CSVs from any data dump should work. I have also provided a sample
dataset in the submission directory at the file path data/speed_layer_sample.csv
to test the speed layer. This sample dataset comes from a second release of
Russian state-sponsored Twitter operations. This dataset was downloaded directly
from Twitter then  limited to 400 rows using the head command. No other
alterations were made.

## Technologies used

#### Batch Layer

- Data on account information is ingested from a CSV into sequence files in HDFS
using a Thrift serialization. The code for this thrift serialization is included
in the submission directory at the file path batch-layer/serialize_users. An
uberjar for this program is included on the compute node of the class cluster at
/home/benfogarty/finalProject/public uber-serialize_users-0.0.1-SNAPSHOT.jar
(and is chmoded appropriately to allow access from a Hive script). The following
commands were used to run this program on a compute node of the class cluster:

    ```
    # Generate necessary HDFS directories
    hdfs dfs -mkdir /benfogarty/finalProject
    hdfs dfs -mkdir /benfogarty/finalProject/users

    # Ingest user data with Thrift Serialization
    yarn jar /home/benfogarty/finalProject/uber-serialize_users-0.0.1-SNAPSHOT.jar com.benfogarty.mpcs53014.HadoopUsersSerialization /home/benfogarty/finalProject/data/ira_users_csv_hashed.csv
    ```

Note that the original CSV file of account information was stored at /home/
benfogarty/finalProject/ira_users_csv_hashed.csv on the class cluster.

- Data from the tweets CSV file was stored in Hive using ORC files, and the
Thrift serialization of user data was registered in Hive as an external table.
The script used to process these two operations is available in the submission
directory at the file path batch-layer/ingest_to_hive.hql. The following
commands were used to run this script on a compute node of the class cluster:

    ```
    # Generate necessary HDFS directory and move tweets CSV to HDFS
    hdfs dfs -mkdir /benfogarty/finalProject/users
    hdfs dfs -put /home/benfogarty/finalProject/data/ira_tweets_csv_hashed.csv /benfogarty/finalProject/tweets/

    # Run the Hive ingestion script
    hive -f /home/benfogarty/finalProject/ingest_to_hive.hql
    ```
    
- A Scala script was used to process the Hive tables and generate batch views
from the datasets. In particular, this script (1) processes tweet text,
including  tokenization, removing stop words, URLs, hashtags, and account
mentions, (2) generates ngrams of length 2, 3, 4, and 5 for all tweets, (3)
aggregates ngram and hashtag usage by month and by account, (4) joints ngram and
hashtag usage by account with the account information table for the accounts
with the most engagement on tweets including a given ngram/hashtag, and (5)
outputs these batch views to Hive. Note that the app only process tweets
identified in the  dataset as being written in English. A copy of this script is
available in the submission directory at the filepath batch-layer/
process_data.scala. The command used to run this script on a compute node of the
class cluster was:

    ```
    # Add the Users serialization to HDFS
    hdfs dfs -put /home/benfogarty/finalProject/public/serialize_users-0.0.1-SNAPSHOT.jar /benfogarty/finalProject

    # Run the Scala script
    spark-shell --conf spark.hadoop.metastore.catalog.default=hive --driver-class-path /home/benfogarty/finalProject/public/serialize_users-0.0.1-SNAPSHOT.jar -i /home/benfogarty/finalProject/process_data.scala
    ```

#### Serving Layer

- Another Hive script was used to move the batch views created in the previous
step to HBase to serve as the serving layer for the app. That script is avaialbe
in the submission directory at the file path load_to_hbase.hql. Before running
this script, I ran the following commands in the hbase shell on a compute node
of the class cluster to generate the necessary HBase tables:

    ```
    # Create necessary HBase tables
    create 'benfogarty_ngrams_by_month','ngrams'
    create 'benfogarty_ngrams_top_users','ngrams'
    create 'benfogarty_hashtags_by_month','hashtags'
    create 'benfogarty_hashtags_top_users','hashtags'
    ```

Then, I ran the aforementioned Hive script using the following command on a
compute node of the class cluster:

    hive -f /home/benfogarty/finalProject/load_to_hbase.hql

#### Speed Layer

- The speed layer for this app allows users to submit a CSV file of new tweets
(using the same format as Twitter provides tweets) and updates the
month-by-month statistics on hashtag and ngram usage. To enable this layer, I
first created a Kafka topic using the follwing command:
    
    ````
    /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper  mpcs53014c10-m-6-20191016152730.us-central1-a.c.mpcs53014-2019.internal:2181 --replication-factor 1 --partitions 1 --topic benfogarty_tweets
    ````

- The speed layer program is a Scala class, and the code for it is provided in
the submisstion directory under the subdirectory speed_layer_tweets/. An uberjar
of this program is located on the class cluster at /home/benfogarty/
user-speed_layer_tweets-0.0.1-SNAPSHOT.jar. Before running this program for the
first time, I ran the following commands in the hbase shell on a compute node of
the class cluster to generate HBase tables for storing my realtime view:

    ```
    # Genrerate necessary HBase tables
    create 'benfogarty_ngrams_speed', 'ngrams'
    create 'benfogarty_hashtags_speed', 'hashtags'
    ```

- To run the speed layer and have it listen for Kafka messages, I ran the
following command from a compute node of the class cluster:

    ```
    spark-submit --class StreamTweets /home/benfogarty/finalProject/uber-speed_layer_tweets-0.0.1-SNAPSHOT.jar mpcs53014c10-m-6-20191016152730.us-central1-a.c.mpcs53014-2019.internal:6667
    ```

- The speed layer is designed to handle malformed input that lacks the proper
column headers gracefully, catching the error in the uploaded data rather than
crashing the speed layer.

#### Web App

- My web app is written in Python with the Flask web framework, happybase HBase
client, and kafka-python Kafka client. A copy of the web app is availabe in the
sumbission directory at the file path webapp/app.py, and the HTML/Javascript
templates for the app are available in the submission directory in the
subdirectory webapp/templates/. All files for the web app are also available
on the webserver node of the class cluster in the directory /home/benfogarty/
finalProject/. The web app includes three pages:
  
  - /tweets.html : homepage for the app allowing users to query the app
  - /results.html : displays results retrieved from the app
  - /submit-tweets.html : allows users to upload a CSV of tweets for the speed
                          layer to process

- The web app was developed using a miniconda installation running Python 3.7.4.
To activate this miniconda environment, I ran the following command on the
webserver node of the class cluster:

    ```
    source /home/benfogarty/miniconda3/bin/activate
    ```

- Then to run the web app, I run the following command:

    ```
    python3 /home/benfogarty/finalProject/app.py
    ```
