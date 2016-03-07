




package com.rock.twitterEventDetector.dbscanTweet

import java.{util, lang}
import java.util.Date

import com.rock.twitterEventDetector.db.mongodb.sparkMongoIntegration.SparkMongoIntegration
import com.rock.twitterEventDetector.db.mongodb.{DbpediaAnnotationCollection, TweetCollection, DbpediaCollection}
import com.rock.twitterEventDetector.model.Tweets.{AnnotatedTweetWithDbpediaResources, AnnotatedTweet, Tweet}
import com.mongodb.casbah.commons.Imports
import com.mongodb. hadoop.{MongoOutputFormat, BSONFileOutputFormat}
import com.rock.twitterEventDetector.lsh.{IndexedRDDLshModel, LSH, LSHModel}
import com.rock.twitterEventDetector.model.Model._
import com.rock.twitterEventDetector.nlp.DbpediaSpootLightAnnotator
import com.rock.twitterEventDetector.nlp.indexing.{AnalyzerUtils, MyAnalyzer}

import com.twitter.chill.Tuple2Serializer
import com.twitter.chill.KSerializer
import edu.berkeley.cs.amplab.spark.indexedrdd.{IndexedRDD, KeySerializer}
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
import com.rock.twitterEventDetector.dbscanTweet.Distances._

import org.apache.hadoop.conf.Configuration
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.mllib.feature.{HashingTF, IDF, Normalizer}
import org.apache.spark.mllib.linalg.{SparseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulable, FutureAction, SparkConf, SparkContext}
import org.bson.BSONObject

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.{Map, mutable}
import scala.collection.parallel.ParSeq



 /**
  * Created by rocco on 26/01/2016.
  */
object TweetClustering {
  def generateCouplesFromList(list:List[Long])={
    @tailrec
    def generateCoupleTailRec(list:List[Long], acc: List[(Long,Long)]):List[(Long,Long)]={
      list match {
        case head::Nil=> acc
        case head :: tail =>
          val couples=tail.map(x=>(head,x))
          // val couples = List(head).zipAll(tail, head, 0)
          generateCoupleTailRec(tail, acc ++ couples)
      }
    }

    generateCoupleTailRec(list, List())
  }

  /**
    * Generate tf-idf vectors from the a rdd containing tweets
    *
    * @param tweets
    * @param sizeDictionary
    * @return
    */
  def generateTfIdfVectors(tweets: RDD[(Long, Tweet)], sizeDictionary: Int): RDD[(Long, SparseVector)] = {

    val hashingTF = new HashingTF(sizeDictionary)
    // Load documents (one per line).
    val tfVectors: RDD[(VertexId, Vector)] = tweets mapPartitions  {
      it => {
        val analyzer = new MyAnalyzer()
        it.flatMap { case (idtweet, tweet) =>
          val tokenList = AnalyzerUtils.tokenizeText(analyzer, tweet.text).asScala

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

        (tuple._1, idf.transform(tuple._2).toSparse)
      }
    }
    tfidf
  }
  /**
    * Generate tf-idf vectors from the a rdd containing tweets
    *
    * @param tweets
    * @param sizeDictionary
    * @return
    */
  def nlpPipeLine(tweets: RDD[(Long, Tweet)], sizeDictionary: Int): RDD[(Long, AnnotatedTweet)] = {

    val hashingTF = new HashingTF(sizeDictionary)
    // Load documents (one per line).
    val tfVectors: RDD[(VertexId, Tweet,Vector,List[DbpediaAnnotation])] = tweets mapPartitions {
      it => {
        val analyzer = new MyAnalyzer()
        val dbpediaSpootligth=new DbpediaSpootLightAnnotator
        it.flatMap { case (idtweet, tweet) =>

          /**
            * per far si che gli hashtag abbiano un boost pari a 2.0
            * è sufficiente appendere a fine del testo del tweet tutti gli hashtag
            * in questo modo avranno tf pari a 2.
            */
          val textToTokenize:String =tweet.text+" "+tweet.splittedHashTags.getOrElse("")+" "+tweet.hashTags.mkString(" ");

          val tokenList = AnalyzerUtils.tokenizeText(analyzer, textToTokenize).asScala

          if (tokenList.size >= 2) {
            val dbpediaAnnotations =
              dbpediaSpootligth.annotateTweet(tweet).getOrElse(List.empty[DbpediaAnnotation])
            Some(idtweet, tweet, hashingTF.transform(tokenList), dbpediaAnnotations)
          }
          else None

        }
      }
    }

    tfVectors.cache()

    val idf = new IDF(2).fit(tfVectors.map(x=>x._3))
    val annotatedTweet = tfVectors.map (tuple =>
      (tuple._1,  new AnnotatedTweet(tuple._2 ,idf.transform(tuple._3).toSparse,tuple._4)))
    annotatedTweet
  }







  def annotatateTweetRDDAndSaveResult( tweets:RDD[(VertexId, Tweet)])= {

tweets.cache()
    val c= tweets.mapPartitions{
      it => {
        val annotator = new DbpediaSpootLightAnnotator
        val anns: Iterator[(Long, List[DbpediaAnnotation])] = it.map {
          case (idtweet, tweet) => {
            val annotations = annotator.annotateText(tweet.text).getOrElse(List.empty[DbpediaAnnotation])
            DbpediaAnnotationCollection.insertDbpediaAnnotationsOfTweet(idtweet,annotations)
            (idtweet, annotations)
          }
        }
       // .inserDbpediaAnnotationsBulk2(anns)
        anns
       // anns.size

      }
    }
   println(c.count())
  }



    /**
      *
      * @param sc
      * @param data
      * @param minPts
      * @param eps
      */
    def startClusteringTweets(sc: SparkContext,
                              data: RDD[(Long, Tweet)],
                              minPts: Int, eps: Double)  = {



      /**
        * primo passo da fare è generare
        * per ogni tweet vettori di hashingTF
        */
      val sizeDictionary = Math.pow(2, 18).toInt

      val tfidfVectors: RDD[(VertexId, SparseVector)] =generateTfIdfVectors(data,sizeDictionary)

      /**
        * creo il modello lsh
        */
      val lsh = new LSH(tfidfVectors, sizeDictionary, numHashFunc = 13, numHashTables = 10)
      val lshModel = lsh.run()
      val indexedLSH=new IndexedRDDLshModel(lshModel)

      //model.save(sc, "target/first72-13r10b")
      data.cache()











      //val tfIdfVectors: RDD[(Long, SparseVector)] = generateTfIdfVectors(data, sizeDictionary)
      //fIdfVectors.cache()
      val annotatedTweets=nlpPipeLine(data,sizeDictionary)

      val annotatedWithResources: Seq[(VertexId, AnnotatedTweetWithDbpediaResources)] =annotatedTweets.map{
        annotatetTweet=> {
          val dbpediaResources: Set[DbpediaResource] =annotatetTweet._2.urisDbpedia.flatMap {
            dbpediaAnnotation: DbpediaAnnotation => DbpediaCollection.findDbpediaResourceByURI(dbpediaAnnotation.uriDBpedia)
          }.toSet
          (annotatetTweet._1,new  AnnotatedTweetWithDbpediaResources(annotatetTweet._2.tweet,annotatetTweet._2.tfIdfVector,dbpediaResources))
        }
      }.collect().toSeq














      //   val annotatedWithResourcesCollected=annotatedWithResources.collect().toSeq
      val objectNeighborsList: Seq[(VertexId, VertexId)] = annotatedWithResources.flatMap {
        case (idTweetA: Long, annotatedTweetA:AnnotatedTweetWithDbpediaResources) =>

          /**
            * retrive the candidate list-neighbors from the lsh model
            */
          val candidateNeighbors: List[Long] =indexedLSH.getCandidateListFromIndexedRDD(idTweetA)

          //lshModel.getCandidates(idTweetA).collect().toList


          val candidateVectors: Seq[(VertexId, AnnotatedTweetWithDbpediaResources)] = annotatedWithResources.filter(x => candidateNeighbors.contains(x._1))

          /**
            * warn!! the candidate list should be filtered to avoid false positive.
            * i.e those
            * object who lies in the same bucket of current object,
            * but whose distance is greater than the given
            * treshold eps
            */
          val neighborList: Seq[(VertexId, VertexId)] = candidateVectors flatMap {
            case (idTweetB, annotatedTweetB) =>
              val timeSimilarity=timeDecayFunction(annotatedTweetA.tweet.createdAt,annotatedTweetB.tweet.createdAt)


                 val cosSim =  cosineSimilarity(annotatedTweetA.tfIdfVector, annotatedTweetB.tfIdfVector)
                if((1-cosSim)<=2*eps){
                  val semanticSim=semanticSimilarity(annotatedTweetA.dbpediaResoruceSet,annotatedTweetB.dbpediaResoruceSet)

                  val similarity=timeSimilarity*((cosSim+semanticSim)/2)
                  val distance=1d-similarity

                  if (distance <= eps)
                    Some(idTweetA, idTweetB)
                  else
                    None
                }else None

          }
          neighborList


      }

      /**
        * a partire dall
        */
      val filteredneighList: RDD[(VertexId, VertexId)] =  sc.parallelize(objectNeighborsList).groupByKey().
        filter(x => x._2.size > minPts).flatMap {
        case (idCore: Long, listNeighbor: Iterable[Long]) => listNeighbor map {
          neighbor => (idCore, neighbor)

        }
      }.cache()

      val graph: Graph[Int, Int] = Graph.fromEdgeTuples(filteredneighList, 1)
      val connectedComponents: VertexRDD[VertexId] = graph.connectedComponents().vertices;


      // objectNeighborsList.foreach(x=>println(x._1+" vicinato "+x._2))
      connectedComponents

    }


   /**
     * esegue il clustering confrontando i tweets solo con la similarità del coseno
     *
     * @param sc
     * @param data
     * @param minPts
     * @param eps
     */
   def startClusteringTweetsCosOnly(sc: SparkContext,
                             data: RDD[(Long, Tweet)],
                             minPts: Int, eps: Double): VertexRDD[VertexId] = {



     /**
       * primo passo da fare è generare
       * per ogni tweet vettori di hashingTF
       */
     val sizeDictionary = Math.pow(2, 18).toInt

     val tfidfVectors: RDD[(VertexId, SparseVector)] =generateTfIdfVectors(data,sizeDictionary)

     val lsh = new LSH(tfidfVectors, sizeDictionary, numHashFunc = 13, numHashTables = 10)
     val lshModel = lsh.run()
     val indexedLSH=new IndexedRDDLshModel(lshModel)

     //model.save(sc, "target/first72-13r10b")
     data.cache()






     val annTweets: Array[(VertexId, (Tweet, SparseVector))] =data.join(tfidfVectors).collect()

     //   val annotatedWithResourcesCollected=annotatedWithResources.collect().toSeq
     val objectNeighborsList: Seq[(VertexId, VertexId)] = annTweets.flatMap {
       case (idTweetA: Long, (tweetA:Tweet,tfIdfVectorA:SparseVector)) =>

         /**
           * retrive the candidate list-neighbors from the lsh model
           */
         val candidateNeighbors: List[Long] =indexedLSH.getCandidateListFromIndexedRDD(idTweetA)

         //lshModel.getCandidates(idTweetA).collect().toList


         val candidateVectors= annTweets.filter(x => candidateNeighbors.contains(x._1))

         /**
           * warn!! the candidate list should be filtered to avoid false positive.
           * i.e those
           * object who lies in the same bucket of current object,
           * but whose distance is greater than the given
           * treshold eps
           */
         val neighborList: Seq[(VertexId, VertexId)] = candidateVectors flatMap {
           case (idTweetB, (tweetB:Tweet,tfIdfVectorB:SparseVector)) =>
             val timeSimilarity=timeDecayFunction(tweetA.createdAt,tweetB.createdAt)


             val cosSim =  cosineSimilarity(tfIdfVectorA,tfIdfVectorB)

             val similarity=timeSimilarity*cosSim




             val distance=1d-similarity
             if(distance<=eps)
              Some(idTweetA,idTweetB)
             else
               None

         }
         neighborList


     }

     /**
       * a partire dall
       */
     val filteredneighList: RDD[(VertexId, VertexId)] =  sc.parallelize(objectNeighborsList).groupByKey().
       filter(x => x._2.size > minPts).flatMap {
       case (idCore: Long, listNeighbor: Iterable[Long]) => listNeighbor map {
         neighbor => (idCore, neighbor)

       }
     }.cache()

     val graph: Graph[Int, Int] = Graph.fromEdgeTuples(filteredneighList, 1)
     val connectedComponents: VertexRDD[VertexId] = graph.connectedComponents().vertices;


     // objectNeighborsList.foreach(x=>println(x._1+" vicinato "+x._2))
     connectedComponents

   }







  def main222(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("clustering")
      .set("spark.executor.memory ", "10g")
      .set("spark.local.dir", "/tmp/spark-temp");
    val sc = new SparkContext(sparkConf)




    val minDAte = TweetCollection.findMinMaxValueDate()
    println("min date value" + minDAte)

    val maxDate = new Date(minDAte.getTime + 10000)
    val tweetsfirst72H: RDD[(VertexId, Tweet)] = SparkMongoIntegration.getTweetsFromDateOffset(sc, minDAte,1)

  }

}


