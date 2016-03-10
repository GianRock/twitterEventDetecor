package com.rock.twitterEventDetector.dbscanTweet

import java.util.Date

import com.rock.twitterEventDetector.db.mongodb.DbpediaAnnotationCollection
import com.rock.twitterEventDetector.db.mongodb.sparkMongoIntegration.SparkMongoIntegration
import com.rock.twitterEventDetector.lsh.{IndexedRDDLshModel, LSH, LSHModel}
import com.rock.twitterEventDetector.model.Model.DbpediaAnnotation
import com.rock.twitterEventDetector.model.Tweets.{AnnotatedTweet, Tweet}
import com.rock.twitterEventDetector.nlp.DbpediaSpootLightAnnotator
import com.rock.twitterEventDetector.nlp.indexing.{AnalyzerUtils, MyAnalyzer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{VertexRDD, Graph, VertexId}
import org.apache.spark.mllib.feature.{Normalizer, IDF, HashingTF}
import org.apache.spark.mllib.linalg.{Vector, SparseVector}
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import scala.collection
import scala.collection.JavaConverters._
import scala.util.Try
import com.rock.twitterEventDetector.dbscanTweet.Distances._


object TweetClustering {
  /**
    *
    * @param eps
    * @param minPts
    * @param data
    * @param lshModel
    * @param sizeDictionary
    * @return
    */
  def apply(eps: Double, minPts: Int, data: RDD[(Long, Tweet)], lshModel: LSHModel, sizeDictionary: Int = 1): TweetClustering = {
    val dim = math.pow(2, sizeDictionary).toInt
    val tfIdf = generateTfIdfVectors(data, dim)
    new TweetClustering(eps, minPts, lshModel, tfIdf)
  }

  /**
    *
    * @param eps
    * @param minPts
    * @param data
    * @param numBands
    * @param numRows
    * @param sizeDictionary
    * @return
    */
  def apply(eps: Double, minPts: Int, data: RDD[(Long, Tweet)], numBands: Int, numRows: Int, sizeDictionary: Int ): TweetClustering = {
    val dim = math.pow(2, sizeDictionary).toInt
    val tfIdf = generateTfIdfVectors(data, dim)
    val model = generateLshModel(numBands, numRows, data)
    new TweetClustering(eps, minPts, model, tfIdf)
  }

  /**
    *
    * @param eps
    * @param minPts
    * @param data
    * @param modelPath
    * @param sc
    * @param sizeDictionary
    * @return
    */
  def apply(eps: Double, minPts: Int, data: RDD[(Long, Tweet)], modelPath: String, sc : SparkContext, sizeDictionary: Int): TweetClustering = {
    val dim = math.pow(2, sizeDictionary).toInt
    val tfIdf = generateTfIdfVectors(data, dim)
    val model = LSHModel.load(sc,modelPath)
    new TweetClustering(eps, minPts, model, tfIdf)
  }

  /**
    *
    * @param numBands
    * @param numRows
    * @param data
    * @param sizeDictionary
    * @return
    */
  private def generateLshModel(numBands: Int , numRows: Int, data: RDD[(Long, Tweet)], sizeDictionary: Int = 18): LSHModel = {
    val dim = math.pow(2, sizeDictionary).toInt
    val tfIdf = generateTfIdfVectors(data, dim)
    val lsh = new LSH(tfIdf, dim, numRows, numBands)
    val lshModel = lsh.run()
    lshModel
  }

  /**
    * This method generate tf-idf vectors for the tweets using the hasing trick
    * in order to generate tokens we adopted some nlp tecniques:
    * <ul>
    *   <li> lowercase filter </li>
    *   <li> stop word-removal </li>
    *   <li> stemming </li>
    * </ul>
    * Remove short tweets and generate tf-idf vectors from remained tweets
    *
    * @param tweets
    * @param sizeDictionary
    * @return
    */
  private def generateTfIdfVectors(tweets: RDD[(Long, Tweet)], sizeDictionary: Int): RDD[(Long, SparseVector)] = {

    val hashingTF = new HashingTF(sizeDictionary)
    // Load documents (one per line).
    val tfVectors: RDD[(VertexId, Vector)] = tweets mapPartitions  {
      it => {
        val analyzer = new MyAnalyzer()
        it.flatMap { case (idtweet, tweet) =>

          /**
            * per far si che gli hashtag abbiano un boost pari a 2.0
            * è sufficiente appendere a fine del testo del tweet tutti gli hashtag
            * in questo modo avranno tf pari a 2.
            */
          val textToTokenize:String =tweet.text+" "+tweet.splittedHashTags.getOrElse("")+" "+tweet.hashTags.mkString(" ");
          val tokenList = AnalyzerUtils.tokenizeText(analyzer, textToTokenize).asScala

          if (tokenList.size >= 2) {
            Some(idtweet, hashingTF.transform(tokenList))

          }
          else None


        }


      }


    }

    tfVectors.cache()
    val idf = new IDF(2).fit(tfVectors.values)

    val norm: Normalizer = new Normalizer()
    val tfidf: RDD[(Long, SparseVector)] = tfVectors.map {
      tuple => {

        (tuple._1, norm.transform(idf.transform(tuple._2)).toSparse)
      }
    }
    tfidf

  }

}

/**
  * Created by rocco on 26/01/2016.
  */
class TweetClustering(eps: Double, minPts: Int, lshModel: LSHModel, tfIdfVectors: RDD[(Long,SparseVector)]) extends Serializable {
  type IdTweet = Long
  type IdCluster = Long

  val indexedLSH = new IndexedRDDLshModel(lshModel)



  /**
    * Generate tf-idf vectors from the a rdd containing tweets
    *
    * @param tweets
    * @param sizeDictionary
    * @return
    */
  private def nlpPipeLine(tweets: RDD[(IdTweet, Tweet)], sizeDictionary: Int): RDD[(IdTweet, AnnotatedTweet)] = {

    val hashingTF = new HashingTF(sizeDictionary)
    // Load documents (one per line).
    val tfVectors: RDD[(IdTweet, Tweet, Vector, List[DbpediaAnnotation])] = tweets mapPartitions { it =>
      val analyzer = new MyAnalyzer()
      val dbpediaSpootligth = new DbpediaSpootLightAnnotator
      it.flatMap {
        case (idtweet, tweet) =>

          /**
            * per far si che gli hashtag abbiano un boost pari a 2.0
            * è sufficiente appendere a fine del testo del tweet tutti gli hashtag
            * in questo modo avranno tf pari a 2.
            */
          //val textToTokenize = tweet.text+" "+tweet.splittedHashTags.getOrElse("")+" "+tweet.hashTags.mkString(" ")
          val textToTokenize = s"${tweet.text} ${tweet.splittedHashTags.getOrElse("")} ${tweet.hashTags.mkString(" ")}"
          val tokenList = AnalyzerUtils.tokenizeText(analyzer, textToTokenize).asScala

          if (tokenList.size >= 2) {
            val dbpediaAnnotations = dbpediaSpootligth.annotateTweet(tweet).getOrElse(List.empty[DbpediaAnnotation])
            Some(idtweet, tweet, hashingTF.transform(tokenList), dbpediaAnnotations)
          }
          else None
      }
    }

    tfVectors.cache()

    val idf = new IDF(2).fit(tfVectors.map(x=>x._3))
    val annotatedTweet = tfVectors.map (tuple =>
      (tuple._1,  new AnnotatedTweet(tuple._2 ,idf.transform(tuple._3).toSparse,tuple._4)))
    annotatedTweet
  }





  /**
    * esegue il clustering confrontando i tweets solo con la similarità del coseno
    */
  def run(data: RDD[(Long, Tweet)],sc: SparkContext): RDD[(VertexId, Date, VertexId)] = {



    /**
      * primo passo da fare è generare
      * per ogni tweet vettori di hashingTF
      */
    val sizeDictionary = Math.pow(2, 18).toInt

    // val tfidfVectors: RDD[(VertexId, SparseVector)] = TweetClustering.generateTfIdfVectors(data,sizeDictionary)

    val annTweets: collection.Map[IdTweet, (Tweet, SparseVector)] = data.join(tfIdfVectors).collectAsMap()

    val objectNeighborsList: List[(IdTweet, IdTweet)] = getSimilarCouples(annTweets)

    val filteredNeighList = getCoreCouples(sc.parallelize(objectNeighborsList))

    val graph: Graph[Int, Int] = Graph.fromEdgeTuples(filteredNeighList, 1)

    val connectedComponents: VertexRDD[VertexId] = graph.connectedComponents().vertices

    val clusteredTweets: RDD[(VertexId, Date, VertexId)] = data.leftOuterJoin(connectedComponents)
      .map{
        case(objectId,(instance, Some(clusterId))) => (objectId, instance.createdAt, clusterId)
        case(objectId,(instance, None)) => (objectId, instance.createdAt,-2L)
      }

    clusteredTweets

  }

  /**
    *
    * @param similarCouples
    * @return
    */
  private def getCoreCouples(similarCouples: RDD[(Long, Long)]): RDD[(Long, Long)] = {
    similarCouples.groupByKey()
      .filter(x => x._2.size > minPts)
      .flatMap {
        case (idCore: Long, listNeighbor: Iterable[Long]) =>
          listNeighbor map (neighbor => (idCore, neighbor))
      }
  }

  private def getSimilarCouples(mapTweets : collection.Map[VertexId, (Tweet, SparseVector)]): List[(Long, Long)] = {


    mapTweets.toList.flatMap {

      case (idTweetA: Long, (tweetA: Tweet, tfIdfVectorA: SparseVector)) =>

        /*
          * retrive the candidate list-neighbors from the lsh model
          */
        val currentTime = System.currentTimeMillis()
        val candidateNeighborsId = indexedLSH.getCandidateListFromIndexedRDD(idTweetA)
        val currentTimeAfterLSH = System.currentTimeMillis()
        val timeQueryLSH=currentTimeAfterLSH-currentTime
        println(" TEMPO RITROVAMENTO DALL LSH "+timeQueryLSH)
        //lshModel.getCandidates(idTweetA).collect().toList

        val candidateVectors = candidateNeighborsId.flatMap { neighborId =>
          val neighborObj = mapTweets.get(neighborId)
          if(neighborObj.isDefined)
            Some(neighborId, neighborObj.get)
          else None

        }

        val currentTimeBeforeComparision=System.currentTimeMillis()

        // val candidateVectors= annTweets.filter(x => candidateNeighbors.contains(x._1))

        /**
          * warn!! the candidate list should be filtered to avoid false positive.
          * i.e those
          * object who lies in the same bucket of current object,
          * but whose distance is greater than the given
          * treshold eps
          */
        val neighborList = candidateVectors flatMap {
          case (idTweetB, (tweetB, tfIdfVectorB)) =>

            val timeSimilarity = timeDecayFunction(tweetA.createdAt,tweetB.createdAt)
            val cosSim =  cosineSimilarity(tfIdfVectorA,tfIdfVectorB)
            val similarity= timeSimilarity * cosSim
            val distance = 1d-similarity

            if(distance<=eps)
              List((idTweetA,idTweetB),(idTweetB,idTweetA))

            else
              List.empty
        }

        println(" NEIGHBOR LIST SIZE "+neighborList.size)
        val currentTimeAfterComparision=System.currentTimeMillis()
        val timeComparisione=currentTimeAfterComparision-currentTimeBeforeComparision
        println(" TEMPO COnfronto viciniato "+timeComparisione)

        neighborList
    }

  }



}

