package com.rock.twitterEventDetector.dbscanTweet

import java.nio.charset.StandardCharsets
import java.nio.file.{OpenOption, Files, Paths}
import java.util.Date

import com.rock.twitterEventDetector.db.mongodb.sparkMongoIntegration.SparkMongoIntegration
import com.rock.twitterEventDetector.lsh.LSH
import com.rock.twitterEventDetector.model.Tweets.Tweet
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime
 import scala.util.Try

/**
  * Created by rocco on 01/03/16.
  */
object Main extends App{
  val maxMemory=             Try(args(0)).getOrElse("10g")
  val numRows              = Try(args(1)).getOrElse("10").toInt
  val numBands             = Try(args(2)).getOrElse("20").toInt
  val minDate=DateTime.parse(Try(args(3)).getOrElse("2012-10-10T14:00:00.000+01:00"))
  val maxDate=DateTime.parse(Try(args(4)).getOrElse("2012-10-11T14:00:00.000+01:00"))
  val dicPow               = Try(args(5)).getOrElse("18").toInt
  val resultFilePath       = Try(args(5)).getOrElse("./results/")



  /**
    *
    */
  val conf = new SparkConf()
    .setAppName("LSH")
    .setMaster("local[*]")
    .set("spark.executor.memory ", maxMemory)
  val sc = new SparkContext(conf)


  val tweets: RDD[(Long, Tweet)] =SparkMongoIntegration.getTweetsAsRDDInTimeInterval(sc,minDate.toDate,maxDate.toDate)
  println("NUMBER OF TWEETS "+tweets.count())

  val dicSize = Math.pow(2.0, dicPow).toInt
  val tfidfVectors = TweetClusteringCosineOnly.generateTfIdfVectors(tweets, dicSize)
  val lsh = new LSH(tfidfVectors, dicSize, numHashFunc=numRows, numHashTables = numBands)
  val lshModel = lsh.run()
  lshModel.save(sc,"./result/lshModel_r"+numRows+"_b"+numBands)
  //val resultEval=TweetClusteringC.zosineOnly.evaluateLSHModel(lshModel,tfidfVectors)


  //Files.write(Paths.get("eva_b"+numBands+"_r"+numRows+"dicSize"+dicPow+"date "+minDate), resultEval.toString().getBytes(StandardCharsets.UTF_8))*/


}
