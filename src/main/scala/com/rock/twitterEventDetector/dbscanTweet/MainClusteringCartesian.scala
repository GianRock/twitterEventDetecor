package com.rock.twitterEventDetector.dbscanTweet

import com.rock.twitterEventDetector.db.mongodb.sparkMongoIntegration.SparkMongoIntegration
import com.rock.twitterEventDetector.lsh.{LSHModelWithData, LSHWithData}
import com.rock.twitterEventDetector.model.Tweets.Tweet
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

import scala.util.Try

/**
  * Created by rocco on 17/03/16.
  */
object MainClusteringCartesian extends  App{


  val maxMemory = Try(args(0)).getOrElse("60G")
  val collectionTweetsName=Try(args(1)).getOrElse("onlyRelevantTweets")

  val dicPow= Try(args(2)).getOrElse("19").toInt
  /**
    * dbscan propreties
    */
  val eps=Try(args(3)).getOrElse("0.35").toFloat
  val minPts=Try(args(4)).getOrElse("10").toInt
  val resultPath=Try(args(5)).getOrElse("i")
  val minDateString=Try(args(6)).getOrElse("2012-10-10T01:00:20Z")
  val maxDateString=Try(args(7)).getOrElse("2012-11-07T00:59:46Z")


  /**
    *
    */
  val conf = new SparkConf()
    .setAppName("LSH")
    .setMaster("local[*]")
    .set("spark.executor.memory ", maxMemory)
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  conf.registerKryoClasses(Array(classOf[com.rock.twitterEventDetector.lsh.Hasher], classOf[Tweet]))
val sc=new SparkContext(conf)

  val minDate=DateTime.parse(minDateString).toDate
  val maxDate=DateTime.parse(maxDateString).toDate
  println(minDate)
  println(maxDate)


  val tweets: RDD[(Long, Tweet)] =SparkMongoIntegration.getTweetsAsRDDInTimeInterval(sc,minDate,maxDate,collectionTweetsName)

  val dicSize = Math.pow(2.0, dicPow).toInt
  val currentTime=System.currentTimeMillis()




  val startTimeClustering=System.currentTimeMillis()
  val clusteredData=TweetClusteringCosineOnly.clusterCartesian(sc,tweets,minPts,eps,dicPow)
  val endTimeCLustering=System.currentTimeMillis()

  val timeCluseringex=endTimeCLustering-startTimeClustering
  println("TEMPO CLUSTERING "+timeCluseringex)


  clusteredData.map(_.productIterator.mkString(",")).coalesce(1).saveAsTextFile("./results/clusterResultsCartesian/"+resultPath+"/clusterData_eps"+eps+"_minPts"+minPts)

/*s
  if(createLshModel)
    lSHModel.save(sc,lshModelTruePath)*/
}
