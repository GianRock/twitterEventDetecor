package com.rock.twitterEventDetector.dbscanTweet

import java.util.Date

import com.rock.twitterEventDetector.model.Model.DbpediaResource
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.SparseVector
import PairWiseDistances._

/**
  * Created by rocco on 01/02/2016.
  */
object Distances {
  val HOUR_MILLISEC=3600000d

  /**
    * given two dates calculates (a,b) a time similarity as follows
    * sim(a,b)=|ha-hb|/72
    * if |ha-hb|<=72h 0 otherwise
    * ha is the number of hours of date since Epoch
    *
    * @param a
    * @param b
    * @return
    */
  def timeDecayFunction(a:Date,b:Date):Double={
    val hourA=a.getTime.toDouble/HOUR_MILLISEC
    val hourB=a.getTime.toDouble/HOUR_MILLISEC
    var hourDist=Math.abs(hourA-hourB)/72d
    if (hourDist > 1) hourDist = 1.0
    1.0-(hourDist)

  }
  /**
    * evaluate the cosine similarity between two
    * tf-idf vectors
    * this similarity range from 0 to 1
    *
    * @param a
    * @param b

    */
  def cosineSimilarity(a: SparseVector, b: SparseVector): Double = {
    var cosine = 0.0

    val intersection = a.indices.intersect(b.indices)
    if (intersection.length > 0) {
      val magnitudeA = a.indices.map(x => Math.pow(a.apply(x), 2)).sum
      val magnitudeB = b.indices.map(x => Math.pow(b.apply(x), 2)).sum
      cosine = intersection.map(x => a.apply(x) * b.apply(x)).sum / (Math.sqrt(magnitudeA) * Math.sqrt(magnitudeB))
    }
    cosine
  }


  def cosine(a: SparseVector, b: SparseVector): Double = {
    var cosine=0.0

    val intersection = a.indices.intersect(b.indices)
    if(intersection.length>0){
      val magnitudeA = a.indices.map(x => Math.pow(a.apply(x), 2)).sum
      val magnitudeB = b.indices.map(x => Math.pow(b.apply(x), 2)).sum
      cosine=intersection.map(x => a.apply(x) * b.apply(x)).sum / (Math.sqrt(magnitudeA) * Math.sqrt(magnitudeB))
    }
    return  cosine
  }
  /**
    * evaluate the similarity between two sets of dbpedia Resources,
    * if one of the sets is empty will return the max similarity
    *
    * @param resourcesA
    * @param resourcesB
    * @return
    */
  def semanticSimilarity(resourcesA:Set[DbpediaResource], resourcesB:Set[DbpediaResource]):Double={
    val semanticSim=if(resourcesA.size>0 && resourcesB.size>0){
      pairWiseSetSim(resourcesA,resourcesB)
    }else 1.0
    semanticSim
  }

  def exacteCosine(a:SparseVector,b:SparseVector):Double={
    1-math.acos(cosine(a,b))/math.Pi
  }

  def  calculateNormalizedGoogleDistanceInt(set1:Set[Int], set2:Set[Int], corpusSize:Int=13994253):Double={

   // require(set1.nonEmpty && set2.nonEmpty," set must be not empty")
    if(set1.isEmpty||set2.isEmpty) 1.0
    else {


      val intersectionSet = set1.intersect(set2)

      val (maxCard, minCard) = if (set1.size > set2.size) (set1.size, set2.size) else (set2.size, set1.size)
      /**
        * se l'intersezione è < 0
        */
      val dist: Double = if (intersectionSet.size > 0) {
        val distance = (Math.log(maxCard) - Math.log(intersectionSet.size)) / (Math.log(corpusSize) - Math.log(minCard))
        if (distance > 1) 1.0
        else distance
      } else 1.0
      dist
    }
  }

  def main(args: Array[String]) {

    val hashingTF = new HashingTF(Integer.MAX_VALUE)
    val c=hashingTF.indexOf("roma")
    println(c)
    println(hashingTF.indexOf("bari"))
  }



}
