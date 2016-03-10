package com.rock.twitterEventDetector.dbscanTweet

import com.rock.twitterEventDetector.db.mongodb.sparkMongoIntegration.SparkMongoIntegration
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime

import scala.util.Try

/**
  * Created by rocco on 09/03/16.
  */

  object MainClustering {

    def main(args: Array[String]) {

      val maxMemory = Try(args(0)).getOrElse("10g")
      val sparkConf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("clustering")
        .set("spark.executor.memory ", maxMemory)
        .set("spark.local.dir", "/tmp/spark-temp");
      val sc = new SparkContext(sparkConf)
      val lshModelPath="./result/lshModel_r14_b30"

      //val lSHModel=LSHModel.load(sc,"./result/lshModel_r14_b30")
      // val lSHModel=LSHModel.load(sc,"target/lshModel_r14_b30")
      val mindateString="2012-10-10T14:00:00.000+01:00"
      val maxDateString="2012-10-11T14:00:00.000+01:00"


      val minDate=DateTime.parse(mindateString).toDate
      val maxDate=DateTime.parse(maxDateString).toDate

      val tweets=SparkMongoIntegration.getTweetsAsRDDInTimeInterval(sc,minDate,maxDate)


      val startTime=System.currentTimeMillis()

      println("NUMBER OF TWEETs"+tweets.count())
      //val clusteredTweets=startClusteringTweetsCosOnly(sc,tweets,lSHModel, 30,0.35)

      val clusteringModel = TweetClustering(eps=0.3, minPts=10, tweets,lshModelPath, sc, 18).run(tweets,sc)

      val endTime=System.currentTimeMillis()

      val executionTime=endTime-startTime

      println(" EXECUTION TIME "+executionTime)
      clusteringModel.saveAsTextFile("./result/clustering2"+System.currentTimeMillis())

    }

}
