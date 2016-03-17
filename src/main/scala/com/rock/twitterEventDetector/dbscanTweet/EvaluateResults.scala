package com.rock.twitterEventDetector.dbscanTweet

import com.rock.twitterEventDetector.db.mongodb.TweetCollection
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by rocco on 10/03/16.
  */
object EvaluateResults {
  def main(args: Array[String]) {
    //  val c= (1 to 500000).par.map(x=>if(x%2==0) '1' else '0').toString()
    //   print(c.par.map(x=>if(x=='0') false else true).toVector)

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("annotations")
      .set("spark.executor.memory ", "14g")
      .set("spark.local.dir", "/tmp/spark-temp");
    val sc = new SparkContext(sparkConf)
    val rddResults=sc.textFile("/home/rocco/IdeaProjects/resources/target/risClustering")
   val results= rddResults.map(x=>x.replaceAll("[()]","").split(",")).map(array=>(array(2),array(0)))
    val clust=results.groupByKey().filter(x=>x._2.size>10)

   val clusteredCOllect= clust.collect()
   val trueClusterdCount: Array[(String, (Iterable[String], Int))] =clusteredCOllect.map{
     case(id,clusterdData)=>(id,(clusterdData,clusterdData.filter(x=>TweetCollection.checkRelevant(x.toLong)).size))
   }.filter(x=>x._2._2>0)

   val csas: Array[(String, Iterable[Long])] = trueClusterdCount.map{
      case(idcluster,(clusters,count))=>
        (idcluster, clusters.flatMap(x=>TweetCollection.findRelevantTweetById(x.toLong)))
    }
    csas.foreach(println)
  }
}
