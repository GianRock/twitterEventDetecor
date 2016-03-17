package com.rock.twitterEventDetector.dbscanTweet

import org.apache.log4j.Logger
import com.rock.twitterEventDetector.db.mongodb.sparkMongoIntegration.SparkMongoIntegration
import com.rock.twitterEventDetector.lsh.{LSH, LSHModel}
import org.apache.log4j.Level
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime

import scala.util.Try

/**
  * Created by rocco on 09/03/16.
  */

  object MainClustering extends App{
   //Logger.getLogger("org").setLevel(Level.ERROR)
 // Logger.getLogger("akka").setLevel(Level.ERROR)

  val maxMemory = Try(args(0)).getOrElse("60g")
  val lshModelPath = Try(args(1)).getOrElse("./result/lshModelReevant_r14_b30")
  val savePath=Try(args(2)).getOrElse("./result/relevantTweetClusterResults")

  val minDateString=Try(args(3)).getOrElse("2012-10-10T01:00:20Z")
  val maxDateString=Try(args(4)).getOrElse("2012-11-07T00:59:46Z")

  val sparkConf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("clustering")
        .set("spark.executor.memory ", maxMemory)
        .set("spark.driver.maxResultSize","8g")
        .set("spark.local.dir", "/tmp/spark-temp");
      val sc = new SparkContext(sparkConf)
     // val lshModelPath="./result/lshModelRelevant_r14_b30"


  val lshModel=LSHModel.load(sc,lshModelPath)




  val minDate=DateTime.parse(minDateString).toDate
  val maxDate=DateTime.parse(maxDateString).toDate
  println(minDate)
  println(maxDate)



  val tweets=SparkMongoIntegration.getTweetsAsRDDInTimeInterval(sc,minDate,maxDate,"onlyRelevantTweets")



  println("NUMBER OF TWEETs"+tweets.count())

  
      val startTime=System.currentTimeMillis()


      val tweetClus = new TweetClustering(eps=0.3, minPts=15)

      val sizeDictionary=Math.pow(2,18).toInt
      val tfIdfVectors=SparkNlpOps.generateTfIdfVectors(tweets,sizeDictionary)
//  val lsh = new LSH(tfIdfVectors, sizeDictionary, numHashFunc=13, numHashTables = 30)
  //val lshModel = lsh.run()
 // lshModel.save(sc,"./result/lshModelRelevant_r"+13+"_b"+30)

   val clusteringMod=tweetClus.run(tweets,tfIdfVectors,lshModel,sc)

      val endTime=System.currentTimeMillis()

      val executionTime=endTime-startTime

      println(" EXECUTION TIME "+executionTime)
     clusteringMod.saveAsTextFile("./result/clusterRelevantResults")



}
