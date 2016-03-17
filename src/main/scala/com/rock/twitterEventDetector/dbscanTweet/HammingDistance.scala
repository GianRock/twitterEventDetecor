package com.rock.twitterEventDetector.dbscanTweet
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
import com.rock.twitterEventDetector.lsh.LSH
import com.rock.twitterEventDetector.model.Tweets.Tweet
import com.rock.twitterEventDetector.nlp.indexing.{AnalyzerUtils, MyAnalyzer}
import com.twitter.chill.Tuple2Serializer
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import org.apache.spark.graphx._
import org.apache.spark.mllib.feature.{Normalizer, IDF, HashingTF}
import org.apache.spark.mllib.linalg.{SparseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.JavaConverters._

import scala.collection.BitSet

/**
  * Created by rocco on 24/02/16.
  */
object HammingDistance {


  /**
    * Generate tf-idf vectors from the a rdd containing tweets
    *
    * @param documents
    * @param sizeDictionary
    * @return
    */
  def generateTfIdfVectors(documents: RDD[(Long, String)], sizeDictionary: Int): RDD[(Long, SparseVector)] = {

    val hashingTF = new HashingTF(sizeDictionary)
    // Load documents (one per line).
    val tfVectors: RDD[(VertexId, Vector)] = documents.mapPartitions {
      it => {
        val analyzer = new MyAnalyzer()
        it.flatMap { case (idtweet, text) =>


          val tokenList = AnalyzerUtils.tokenizeText(analyzer, text).asScala


          if (tokenList.size > 0) {
            Some(idtweet, hashingTF.transform(tokenList))

          }
          else None


        }


      }


    }
    println("TF VECTORS  " )
    tfVectors.collect().foreach(println)
   // println(tfVectors.count())

    tfVectors.cache()
    val idf = new IDF( ).fit(tfVectors.values)

    val norm: Normalizer = new Normalizer()
    val tfidf: RDD[(Long, SparseVector)] = tfVectors.map {
      tuple => {

        (tuple._1, norm.transform(idf.transform(tuple._2)).toSparse )
      }
    }
    println("TFIDF VECTORS  " )
    tfidf.collect().foreach(println)
    tfidf
  }


  // Calculate a sum of set bits of XOR'ed bytes
  def hammingDistance(b1: String, b2: String) = {
    ((b1 zip b2) count (x => x._1 != x._2))
  }

  // Calculate a sum of set bits of XOR'ed bytes


  def stringVectorNorm(vectorString: String) = {
    Math.sqrt(vectorString.filter(x => x == '1').length.toDouble)
  }


  def cosineSigSim(b1: String, b2: String): Double = {
    val innerProduct = ((b1 zip b2) count (x => x._1 == '1' && x._1 == x._2)).toDouble
    val norm1 = stringVectorNorm(b1)
    val norm2 = stringVectorNorm(b2)
    println(innerProduct)
    println(norm1)
    println(norm2)
    innerProduct / (norm1 * norm2)

  }


  /**
    * Approximates the cosine distance of two bit sets using their hamming
    * distance
    *
    */
  def hammingToCosine(a: String, b: String): Double = {
    val hamming = hammingDistance(a, b)
    val pr = 1.0 - (hamming.toDouble / a.size.toDouble)
    //println(pr)
    (1.0 - pr) * (math.Pi)
  }

  def cosine(a: SparseVector, b: SparseVector): Double = {
    var cosine = 0.0

    val intersection = a.indices.intersect(b.indices)
    if (intersection.length > 0) {
      val magnitudeA = a.indices.map(x => Math.pow(a.apply(x), 2)).sum
      val magnitudeB = b.indices.map(x => Math.pow(b.apply(x), 2)).sum
      cosine = intersection.map(x => a.apply(x) * b.apply(x)).sum / (Math.sqrt(magnitudeA) * Math.sqrt(magnitudeB))
    }
    return cosine
  }


  def main(args: Array[String]) = {


    //  val c= (1 to 500000).par.map(x=>if(x%2==0) '1' else '0').toString()
    //   print(c.par.map(x=>if(x=='0') false else true).toVector)

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("annotations")
      .set("spark.executor.memory ", "14g")
      .set("spark.local.dir", "/tmp/spark-temp");
    val sc = new SparkContext(sparkConf)



    val strings = List("VP Biden on Afghanistan: We are leaving in 2014. Period. lol what I just said! "



      , "Where's Biden's choker collar 2 stop him from laughing??  VP Debate",
      "Where's Biden's choker collar  ",


      "Where's Biden's choker collar 2 stop him from laughing??  VP Debate")


    val zippedStrings = (strings zipWithIndex).map(x => (x._2.toLong, x._1))



    println(zippedStrings.size)

    val rdd: RDD[(Long, String)] = sc.parallelize(zippedStrings)
    val dicSize = Math.pow(2.0, 19).toInt
    val tfidf = generateTfIdfVectors(rdd, dicSize)
    val lsh = new LSH(tfidf, dicSize, numHashFunc  =30, numHashTables = 10)
    val lshModel = lsh.run(sc)
    tfidf.collect().foreach(println)
    val cosineSim = tfidf.cartesian(tfidf)

      .filter {
      case ((id1, _), (id2, _)) => id1 < id2
    }.map {
      case ((id1, v1), (id2, v2)) => {
        val cosineSim = cosine(v1, v2)
       (id1, id2, 1 - cosineSim)


      }
    }.collect()
    cosineSim.foreach(println)


    /**
      *a partire dal modello lsh
      * creo un indexed rdd che ad ogni id documento
      * associa un interable di coppie (banda,signature)
      */
    val invertedLsh: RDD[(VertexId, String)] =lshModel.hashTables.map{
      case(hashkey,id)=>(id,hashkey)
    }.groupByKey().map{
      case(id,listHash:Iterable[(Int,String)])=>{
        val sortedHashes: List[(Int, String)] =listHash.toList.sortBy(_._1)
        val signature= sortedHashes.foldLeft("")((b,a) => b+a)
        //val boolanSig: IndexedSeq[Boolean] =signature.map(x=>if (x=='0') false else true)

        (id,signature)
        //  val c=sortedHashes.foldRight()
      }
    }




    val indexedInvertedLsh: IndexedRDD[Long, String] = IndexedRDD(invertedLsh).cache()

    // Calculate a sum of set bits of XOR'ed bytes
    def hammingDistance(b1:String, b2: String):Double = {
      ((b1 zip b2) count ( x => x._1 != x._2 )).toDouble/b1.length.toDouble


    }






    val errors=cosineSim.map{
      case(ida,idb,cosineDistance)=>{
        val signatureA =indexedInvertedLsh.get(ida).get
        val signatureB=indexedInvertedLsh.get(idb).get
        val hamm=hammingDistance(signatureA,signatureB)
        val approximateCosine: Double =hammingDistance( signatureA,signatureB)
        val error=math.abs((cosineDistance-approximateCosine))
        // ((ida,idb)+ "COSINE DISTANCE "+cosineDistance+ " approx "+approximateCosine+" ERROR :"+error,error)
        error
      }
    }


    val avg=errors.reduce(_+_)/errors.length
    val dev=Math.sqrt(errors.map(x=>Math.pow(x-avg,2d)).reduce(_+_)/errors.length)
    println(" AVG ERROR  "+ avg + " MAX ERROR "+errors.max+" STD DEV "+dev)

  }

}
