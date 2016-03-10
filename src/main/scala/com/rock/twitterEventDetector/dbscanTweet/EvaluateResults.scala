package com.rock.twitterEventDetector.dbscanTweet

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
    val clust=results.groupByKey().map(x=>(x._1,x._2.size)).filter(x=>x._2>100).collect().foreach(println)

  }
}
