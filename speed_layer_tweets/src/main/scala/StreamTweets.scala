import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Table
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.{RegexTokenizer, NGram, StopWordsRemover}

object StreamTweets {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

  // Use the following two lines if you are building for the cluster
  hbaseConf.set(
    "hbase.zookeeper.quorum",
    "mpcs53014c10-m-6-20191016152730.us-central1-a.c.mpcs53014-2019.internal"
  )
  hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")

  // Use the following line if you are building for the VM
  // hbaseConf.set("hbase.zookeeper.quorum", "localhost")

  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val nGramsTable =
    hbaseConnection.getTable(TableName.valueOf("benfogarty_ngrams_speed"))
  val hashtagsTable =
    hbaseConnection.getTable(TableName.valueOf("benfogarty_hashtags_speed"))

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamFlights <brokers> 
        |  <brokers> is a list of one or more Kafka brokers
        | 
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("BenFogartyStreamTweets")
    val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("benfogarty_tweets")
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc,
        kafkaParams,
        topicsSet
      )
      
    println("Listener is up!")

    // Define constants across messages from Kafka
    val urlPattern =
      "((\\w+:\\/\\/)[-a-zA-Z0-9:@;?&=\\/%\\+\\.\\*!'\\(\\),\\$_\\{\\}\\^~\\[\\]`#|]+)"
    val rtPattern = "rt @[^:]+: "
    val handlePattern = "@[^ ]+"
    val hashtagPattern = "#\\w+"
    val regexMatch = ("(" + urlPattern + ")|(" + rtPattern + ")|(" +
      handlePattern + ")|(" + hashtagPattern + ")")

    val regexTokenizer = new RegexTokenizer()
      .setInputCol("raw_tweet_text")
      .setOutputCol("raw_tweet_words")
      .setPattern("[\\W-']")

    var stopWordsRemover = new StopWordsRemover()
      .setStopWords(StopWordsRemover.loadDefaultStopWords("english"))
      .setInputCol("raw_tweet_words")
      .setOutputCol("processed_tweet_words")

    val nGramEngagementWindow =
      Window.partitionBy("ngram").orderBy(desc("total_engagement_count"))

    // Process tweets
    val tweetsRDD = messages.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        println("New data recieved!")
        try {
          val processed_rdd = rdd.map(_._2)

          val tweets = sparkSession.read.json(processed_rdd)

          val tweetsText = tweets
            .select(
              date_format(tweets("tweet_time"), "MM-YYYY").as("tweet_month"),
              lower(tweets("tweet_text")).as("raw_tweet_text"),
              tweets("userid"),
              split(regexp_replace(tweets("hashtags"), "\\[|\\]", ""), ", ")
                .as("hashtags"),
              tweets("quote_count").cast(IntegerType),
              tweets("reply_count").cast(IntegerType),
              tweets("like_count").cast(IntegerType),
              tweets("retweet_count").cast(IntegerType)
            )
            .filter(tweets("tweet_language") === "en")
            .filter(tweets("tweet_text").isNotNull)
            .filter(tweets("tweet_time").isNotNull)

          // Processing nGrams
          val tweetsCleanText = tweetsText.select(
            tweetsText("tweet_month"),
            regexp_replace(tweetsText("raw_tweet_text"), regexMatch, "")
              .as("raw_tweet_text"),
            tweetsText("userid"),
            tweetsText("quote_count"),
            tweetsText("reply_count"),
            tweetsText("like_count"),
            tweetsText("retweet_count")
          )

          val tweetsRawWords = regexTokenizer.transform(tweetsCleanText)

          val tweetsProcessedWords = stopWordsRemover.transform(tweetsRawWords)

          for (n <- 2 to 5) {

            val ngramTransformer = new NGram()
              .setN(n)
              .setInputCol("processed_tweet_words")
              .setOutputCol("tweet_text_ngrams")

            val tweetsNGrams = ngramTransformer.transform(tweetsProcessedWords)

            val ngrams = tweetsNGrams.select(
              tweetsNGrams("tweet_month"),
              explode(tweetsNGrams("tweet_text_ngrams")).as("ngram"),
              tweetsNGrams("userid"),
              tweetsNGrams("quote_count"),
              tweetsNGrams("reply_count"),
              tweetsNGrams("like_count"),
              tweetsNGrams("retweet_count")
            )

            val nGramsByMonth = ngrams
              .groupBy(ngrams("tweet_month"), ngrams("ngram"))
              .agg(
                count("*").as("count"),
                sum(ngrams("quote_count")).as("total_quote_count"),
                sum(ngrams("reply_count")).as("total_reply_count"),
                sum(ngrams("like_count")).as("total_like_count"),
                sum(ngrams("retweet_count")).as("total_retweet_count")
              )

            val nGramsOutput = nGramsByMonth.select(
              concat(
                nGramsByMonth("ngram"),
                lit("_"),
                nGramsByMonth("tweet_month").cast(StringType)),
              nGramsByMonth("tweet_month").cast(StringType),
              nGramsByMonth("ngram").cast(StringType),
              nGramsByMonth("count").cast(IntegerType),
              nGramsByMonth("total_quote_count").cast(IntegerType),
              nGramsByMonth("total_reply_count").cast(IntegerType),
              nGramsByMonth("total_like_count").cast(IntegerType),
              nGramsByMonth("total_retweet_count").cast(IntegerType)
            )

            // Writing to HBase
            val writenNGramsUpdate = nGramsOutput.foreach(
              row => checkAndPut(row, nGramsTable, Bytes.toBytes("ngrams"))
            )

          }

          // Processing Hashtags
          val tweetsHashtags = tweetsText.select(
            tweetsText("tweet_month"),
            explode(tweetsText("hashtags")).as("hashtag"),
            tweetsText("userid"),
            tweetsText("quote_count"),
            tweetsText("reply_count"),
            tweetsText("like_count"),
            tweetsText("retweet_count")
          )

          val tweetsProcessedHashtags = tweetsHashtags
            .select(
              tweetsHashtags("tweet_month"),
              lower(tweetsHashtags("hashtag")).as("hashtag"),
              tweetsHashtags("userid"),
              tweetsHashtags("quote_count"),
              tweetsHashtags("reply_count"),
              tweetsHashtags("like_count"),
              tweetsHashtags("retweet_count")
            )
            .filter(tweetsHashtags("hashtag") =!= "")

          val hashtagsByMonth = tweetsProcessedHashtags
            .groupBy(
              tweetsProcessedHashtags("tweet_month"),
              tweetsProcessedHashtags("hashtag")
            )
            .agg(
              count("*").as("count"),
              sum(tweetsProcessedHashtags("quote_count"))
                .as("total_quote_count"),
              sum(tweetsProcessedHashtags("reply_count"))
                .as("total_reply_count"),
              sum(tweetsProcessedHashtags("like_count")).as("total_like_count"),
              sum(tweetsProcessedHashtags("retweet_count"))
                .as("total_retweet_count")
            )
            
         val hashtagsOutput = hashtagsByMonth.select(
              concat(
                hashtagsByMonth("hashtag"),
                lit("_"),
                hashtagsByMonth("tweet_month").cast(StringType)),
              hashtagsByMonth("tweet_month").cast(StringType),
              hashtagsByMonth("hashtag").cast(StringType),
              hashtagsByMonth("count").cast(IntegerType),
              hashtagsByMonth("total_quote_count").cast(IntegerType),
              hashtagsByMonth("total_reply_count").cast(IntegerType),
              hashtagsByMonth("total_like_count").cast(IntegerType),
              hashtagsByMonth("total_retweet_count").cast(IntegerType)
            )

          val writeHashtagsUpdate = hashtagsOutput.foreach(
            row => checkAndPut(row, hashtagsTable, Bytes.toBytes("hashtags"))
          )
          println("Updates processed successfully!")

        } catch {
          case e =>
          println("Couldn't interpret file upload. Are you missing a required column header?")
        }
      }
    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

// Function to update HBase
  def checkAndPut(row: Row, table: Table, cf: Array[Byte]): Unit = {
    val rowKey = Bytes.toBytes(row.getString(0))
    val inc = new Increment(rowKey)
    inc.addColumn(cf, Bytes.toBytes("count#b"), row.getAs[Int](3).toLong)
    inc.addColumn(cf, Bytes.toBytes("total_quote_count#b"), row.getAs[Int](4).toLong)
    inc.addColumn(cf, Bytes.toBytes("total_reply_count#b"), row.getAs[Int](5).toLong)
    inc.addColumn(cf, Bytes.toBytes("total_like_count#b"), row.getAs[Int](6).toLong)
    inc.addColumn(cf, Bytes.toBytes("total_retweet_count#b"), row.getAs[Int](7).toLong)
    table.increment(inc)
  }
}
