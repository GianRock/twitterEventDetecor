package com.rock.twitterEventDetector.dbscanTweet

import com.rock.twitterEventDetector.db.mongodb.TweetCollection
import com.rock.twitterEventDetector.db.mongodb.sparkMongoIntegration.SparkMongoIntegration
import org.apache.spark.rdd.RDD
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
    val rddResults: RDD[String] =sc.textFile("/home/rocco/clusterData_eps0.35_minPts10_b70_r13")

    println(rddResults.count())

    val myResults: RDD[(Long, Long)] =rddResults.map{
      x=>{ val g: Array[String] =x.split(",")
        val cluster=g(1).toLong

        (g(0).toLong,  if (cluster==(-2L))-1L else cluster)
      }

    }//.filter(x=>x._2.equals(-1L)==false)


    val d= myResults.map(_.productIterator.mkString("\t")).coalesce(1)//.saveAsTextFile("results/reseps0.3515w")
    rddResults.collect().foreach(println)
    val  trueRes=SparkMongoIntegration.getRelevantTweets(sc)
    println("TRUE RES"+trueRes.count())
    trueRes.collect().foreach(println)


   myResults.join(trueRes).map(x=>x._1+"\t"+x._2.productIterator.mkString("\t")).coalesce(1).saveAsTextFile("/home/rocco/phytonNotebook/clusterResults/clusterData_eps0.35_minPts10_b70_r13")



     //.map(_.productIterator.mkString("\t").coalesce(1).saveAsTextFile("results/risults")






    /**
      * val results= rddResults.map(x=>x.replaceAll("[()]","").split(",")).map(array=>(array(2),array(0)))
      * val clust=results.groupByKey().filter(x=>x._2.size>10)

      * val clusteredCOllect= clust.collect()
      * val trueClusterdCount: Array[(String, (Iterable[String], Int))] =clusteredCOllect.map{
      * case(id,clusterdData)=>(id,(clusterdData,clusterdData.filter(x=>TweetCollection.checkRelevant(x.toLong)).size))
      * }.filter(x=>x._2._2>0)

      * val csas: Array[(String, Iterable[Long])] = trueClusterdCount.map{
      * case(idcluster,(clusters,count))=>
      * (idcluster, clusters.flatMap(x=>TweetCollection.findRelevantTweetById(x.toLong)))
      * }
      * csas.foreach(println)
      * }*/
}
}