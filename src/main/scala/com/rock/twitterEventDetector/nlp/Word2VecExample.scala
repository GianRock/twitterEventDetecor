package com.rock.twitterEventDetector.nlp

import com.rock.twitterEventDetector.db.mongodb.sparkMongoIntegration.SparkMongoIntegration
import com.rock.twitterEventDetector.dbscanTweet.Main._
import com.rock.twitterEventDetector.nlp.indexing.{AnalyzerUtils, MyAnalyzer}
import org.apache.spark._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.joda.time.DateTime
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

/**
  * Created by rocco on 02/03/16.
  */
object Word2VecExample {

  def sumArray (m: Array[Double], n: Array[Double]): Array[Double] = {
    for (i <- 0 until m.length) {m(i) += n(i)}
    return m
  }
  def divArray (m: Array[Double], divisor: Double) : Array[Double] = {
    for (i <- 0 until m.length) {m(i) /= divisor}
    return m
  }

  def wordToVector (w:String, m: Word2VecModel): Vector = {
    try {
      return m.transform(w)
    } catch {
      case e: Exception => return Vectors.zeros(100)
    }
  }



  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("LSH")
      .setMaster("local[*]")
      .set("spark.executor.memory ", "10g")
    val sc = new SparkContext(conf)

    val minDate=DateTime.parse("2012-11-05T19:40:00.000+01:00")
    val maxDate=DateTime.parse("2012-11-05T20:00:00.000+01:00")
    val tweets=SparkMongoIntegration.getTweetsAsRDDInTimeInterval(sc,minDate.toDate,maxDate.toDate)

    tweets.cache()
print("NUMBER OF TWEETS"+tweets.count())





val texts: RDD[List[String]] =  tweets.map{
    case(id,tweet)=>
      {
       val analyzer = new MyAnalyzer()
        /**
          * per far si che gli hashtag abbiano un boost pari a 2.0
          * è sufficiente appendere a fine del testo del tweet tutti gli hashtag
          * in questo modo avranno tf pari a 2.
          */
        val textToTokenize:String =tweet.text+" "+tweet.splittedHashTags.getOrElse("")+" "+tweet.hashTags.mkString(" ");
         AnalyzerUtils.tokenizeText(analyzer, textToTokenize).asScala.toList
      }
  }

    texts.take(10).foreach(println)


    val word2vec = new Word2Vec()

    val model = word2vec.fit(texts)

    val synonyms = model.findSynonyms("talk", 10)

    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }



    val tweetVectors: RDD[Vector] =
      texts.map(
        x => new DenseVector(divArray(x.map(m => wordToVector(m, model).toArray).reduceLeft(sumArray),x.length))
          .asInstanceOf[Vector])


    var numClusters = 10
    val numIterations = 100
    var clusters = KMeans.train(tweetVectors, numClusters, numIterations)
    var wssse = clusters.computeCost(tweetVectors)


    val cluster_centers = sc.parallelize(clusters.clusterCenters.zipWithIndex.map{ e => (e._2,e._1)})
    val cluster_topics: RDD[(Int, List[String])] = cluster_centers.mapValues(x => model.findSynonyms(x,2).map(x => x._1).toList)
    cluster_topics.collect().foreach(println)
    // Save and load model
    //model.save(sc, "myModelPath")
   // val sameModel = Word2VecModel..(sc, "myModelPath")
  }

}
