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
import com.rock.twitterEventDetector.dbscanTweet.SparkNlpOps._


/**
  * Created by rocco on 26/01/2016.
  */
class TweetClustering(eps: Double, minPts: Int) extends Serializable {
  type IdTweet = Long
  type IdCluster = Long










  /**
    * esegue il clustering confrontando i tweets solo con la similarità del coseno
    */
  def run(data: RDD[(Long, Tweet)],tfIdfVectors:RDD[(Long,SparseVector)],lshModel: LSHModel, sc: SparkContext): RDD[(IdTweet, Date, IdCluster)] = {

    val indexedLSH = new IndexedRDDLshModel(lshModel)



    // val tfidfVectors: RDD[(VertexId, SparseVector)] = TweetClustering.generateTfIdfVectors(data,sizeDictionary)

    val annTweets: collection.Map[IdTweet, (Tweet, SparseVector)] = data.join(tfIdfVectors).collectAsMap()

    val objectNeighborsList: List[(IdTweet, IdTweet)] = getSimilarCouples(indexedLSH,annTweets)
    val neighborRDD=sc.parallelize(objectNeighborsList)


    val filteredNeighList = getCoreCouples(neighborRDD)

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
    def getCoreCouples(similarCouples: RDD[(Long, Long)]): RDD[(Long, Long)] = {
    similarCouples.groupByKey()
      .filter{ case(_,neighborhood)=>neighborhood.size>=minPts}
      .flatMap {
        case (idCore:Long, listNeighbor:Iterable[Long]) =>
          listNeighbor map{neighbor=>(idCore,neighbor)}
      }
  }

  /**
    * thi method get for each tweeet the candidate neighbors from lhmodel
    * and filter only the ones whose distance is below the trehsold eps.
    * @param indexedLSH
    * @param mapTweets
    * @return
    */
  protected def getSimilarCouples(indexedLSH:IndexedRDDLshModel,mapTweets : collection.Map[VertexId, (Tweet, SparseVector)]): List[(Long, Long)] = {


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

