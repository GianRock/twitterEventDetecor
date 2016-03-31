package com.rock.twitterEventDetector.dbscanTweet

import com.rock.twitterEventDetector.db.mongodb.sparkMongoIntegration.SparkMongoIntegration



 import com.rock.twitterEventDetector.lsh.{LSHModelWithData, LSHWithData, LSHModel}
import com.rock.twitterEventDetector.model.Tweets.Tweet
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime

import scala.util.Try

/**
  * Created by rocco on 17/03/16.
  */
object MainClusteringCosineOnly extends  App{


  val maxMemory = Try(args(0)).getOrElse("60G")
  val collectionTweetsName=Try(args(1)).getOrElse("onlyRelevantTweets")
  val numRows=Try(args(2)).getOrElse("14").toInt
  val numBands=Try(args(3)).getOrElse("60").toInt

  val dicPow= Try(args(4)).getOrElse("19").toInt


  val lshModelPath = Try(args(5)).getOrElse("./result/lshModel"+collectionTweetsName)
  val createLshModel=Try(args(6)).getOrElse("true").toBoolean


  /**
    * dbscan propreties
    */
  val eps=Try(args(7)).getOrElse("0.25").toFloat
  val minPts=Try(args(8)).getOrElse("15").toInt
  val savePath=Try(args(9)).getOrElse("i")
  val minDateString=Try(args(10)).getOrElse("2012-10-10T01:00:20Z")
  val maxDateString=Try(args(11)).getOrElse("2012-11-07T00:59:46Z")

val lshModelTruePath=lshModelPath+"_b"+numBands+"_r"+numRows

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

  val lSHModel:LSHModelWithData=
  if(createLshModel){
    val tfidfVectors = TweetClusteringCosineOnly.generateTfIdfVectors(tweets, dicSize)
    val lsh = new LSHWithData(tfidfVectors, dicSize, numHashFunc=numRows, numHashTables = numBands)
    lsh.run(sc)

  }else{
    LSHModelWithData.load(sc,lshModelTruePath)
  }

  val endTimeLsh=System.currentTimeMillis()
  val exTimeLSH=endTimeLsh-currentTime
  println("TEMPO GENERAZIONE LSH MODEL "+exTimeLSH)


  val startTimeClustering=System.currentTimeMillis()
  val clusteredData=TweetClusteringCosineOnly.clusteringTweets(sc,tweets,lSHModel,minPts,eps)
  val endTimeCLustering=System.currentTimeMillis()

  val timeCluseringex=endTimeCLustering-startTimeClustering
  println("TEMPO CLUSTERING "+timeCluseringex)
  clusteredData.map(_.productIterator.mkString(",")).coalesce(1).saveAsTextFile("./results/clusterResults/"+savePath+"/clusterDataWind_eps"+eps+"_minPts"+minPts+"_b"+numBands+"_r"+numRows)

/*s
  if(createLshModel)
    lSHModel.save(sc,lshModelTruePath)*/
}
