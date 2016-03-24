package com.rock.twitterEventDetector.dbscanTweet

import java.nio.charset.StandardCharsets
import java.nio.file.{OpenOption, Files, Paths}
import java.util.Date

import com.rock.twitterEventDetector.db.mongodb.sparkMongoIntegration.SparkMongoIntegration
import com.rock.twitterEventDetector.lsh.{LSHModelWithData, LSHWithData, LSH}
import com.rock.twitterEventDetector.model.Tweets.Tweet
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime
 import scala.util.Try

/**
  * Created by rocco on 01/03/16.
  */
object Main extends App{
  val maxMemory=             Try(args(0)).getOrElse("12g")
  val numRows              = Try(args(1)).getOrElse("14").toInt
  val numBands             = Try(args(2)).getOrElse("60").toInt
  val minDate=DateTime.parse(Try(args(30)).getOrElse("2011-10-10T14:00:00.000+01:00"))
  val maxDate=DateTime.parse(Try(args(31)).getOrElse("2013-10-11T14:00:00.000+01:00"))
  val dicPow               = Try(args(5)).getOrElse("19").toInt

  val resultFilePath       = Try(args(6)).getOrElse("./results/")



  /**
    *
    */
  val conf = new SparkConf()
    .setAppName("LSH")
    .setMaster("local[*]")
    .set("spark.executor.memory ", maxMemory)
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
   conf.registerKryoClasses(Array(classOf[com.rock.twitterEventDetector.lsh.Hasher], classOf[Tweet]))


  val sc = new SparkContext(conf)


  val tweets: RDD[(Long, Tweet)] =SparkMongoIntegration.getTweetsAsRDDInTimeInterval(sc,minDate.toDate,maxDate.toDate,"onlyRelevantTweets")
 tweets.cache()
   println("NUMBER OF TWEETS "+tweets.count())

  val dicSize = Math.pow(2.0, dicPow).toInt

  val path="target/relevantLshModelWithData_r"+numRows+"_b"+numBands
  val tfidfVectors = TweetClusteringCosineOnly.generateTfIdfVectors(tweets, dicSize)
 //  val lsh = new LSHWithData(tfidfVectors, dicSize, numHashFunc=numRows, numHashTables = numBands)
  val currentTime=System.currentTimeMillis()
  val lshModel =  LSHModelWithData.load(sc,path)
 // val lshModel=lsh.run(sc)
  val endTime=System.currentTimeMillis()
  val exTime=endTime-currentTime
  println("TEMPO GENERAZIONE LSH MODEL "+exTime)


  val startTimeClustering=System.currentTimeMillis()
  val clusteredData=TweetClusteringCosineOnly.clusteringTweets(sc,tweets,lshModel,10,0.35)
  val endTimeCLustering=System.currentTimeMillis()

  val timeCluseringex=endTimeCLustering-startTimeClustering
  println("TEMPO CLUSTERING "+timeCluseringex)
  clusteredData.map(_.productIterator.mkString(",")).saveAsTextFile(resultFilePath+"/clusterData")

  //shModel.save(sc,path)

 /*
  val lshModel =  LSHModelWithData.load(sc,path) //lsh.run(sc) //LSHModelWithData.load(sc,path)
  println(lshModel.numHashFunc)
  val endTime=System.currentTimeMillis()
  val exTime=endTime-currentTime
  println("TEMPO GENERAZIONE LSH MODEL "+exTime)

 // lshModel.save(sc,path)
  //val resultEval=TweetClusteringC.zosineOnly.evaluateLSHModel(lshModel,tfidfVectors)


  //Files.write(Paths.get("eva_b"+numBands+"_r"+numRows+"dicSize"+dicPow+"date "+minDate), resultEval.toString().getBytes(StandardCharsets.UTF_8))*/


}
