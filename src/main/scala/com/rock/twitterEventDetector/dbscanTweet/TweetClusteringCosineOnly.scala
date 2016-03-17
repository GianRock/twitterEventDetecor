



package com.rock.twitterEventDetector.dbscanTweet

import java.util.Date

import com.rock.twitterEventDetector.db.mongodb.sparkMongoIntegration.SparkMongoIntegration
import com.rock.twitterEventDetector.db.mongodb.{DbpediaAnnotationCollection, TweetCollection}
import com.rock.twitterEventDetector.dbscanTweet.Distances._
import com.rock.twitterEventDetector.lsh.{LSHWithData, LSHModelWithData, LSH, LSHModel}
import com.rock.twitterEventDetector.model.Model._
import com.rock.twitterEventDetector.model.Tweets.{VectorTweet, AnnotatedTweet, AnnotatedTweetWithDbpediaResources, Tweet}
import com.rock.twitterEventDetector.nlp.DbpediaSpootLightAnnotator
import com.rock.twitterEventDetector.nlp.indexing.{AnalyzerUtils, MyAnalyzer}
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.mllib.feature.{HashingTF, IDF, Normalizer}
import org.apache.spark.mllib.linalg.{SparseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.immutable.IndexedSeq
import com.rock.twitterEventDetector.utils.UtilsFunctions._

import scala.collection.mutable

/**
  * Created by rocco on 26/01/2016.
  */
object TweetClusteringCosineOnly {

  val NOISE: VertexId =(-2l)





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


  /**
    * Generate tf-idf vectors from the a rdd containing tweets
    *
    * @param tweets
    * @param sizeDictionary
    * @return
    */
  def generateTfIdfVectors2(tweets: RDD[(Long, Tweet)], sizeDictionary: Int): RDD[Vector] = {

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
    val tfidf = tfVectors.map {
      tuple => {

        (idf.transform(tuple._2))
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
  def nlpPipeLine(tweets: RDD[(Long, Tweet)], sizeDictionary: Int): RDD[(Long, VectorTweet)] = {

    val hashingTF = new HashingTF(sizeDictionary)
    // Load documents (one per line).
    val tfVectors: RDD[(VertexId, Tweet,Vector)] = tweets mapPartitions {
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

            Some(idtweet, tweet, hashingTF.transform(tokenList))
          }
          else None

        }
      }
    }

    tfVectors.cache()

    val idf = new IDF(2).fit(tfVectors.map(x=>x._3))
    val annotatedTweet = tfVectors.map (tuple =>
      (tuple._1,  new VectorTweet(tuple._2 ,idf.transform(tuple._3).toSparse)))
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
    * @param minPts
    * @param eps
    */
  def clusteringTweets(sc: SparkContext,
                       lshModel: LSHModelWithData,
                       minPts: Int,
                       eps: Double):VertexRDD[VertexId]  = {


    /**
      * aggrego gli oggetti nello stesso bucket (idbanda-signature)
      */
    val zeroElem = collection.mutable.LinkedList.empty[(Long,SparseVector)]

    val groupedLSH: RDD[((Int, String), mutable.LinkedList[(Long, SparseVector)])] =lshModel.hashTables.aggregateByKey(zeroElem)(
      (list: collection.mutable.LinkedList[(Long,SparseVector)], v:(Long,SparseVector)) => list.:+(v) ,
      (list1, list2) => list1 ++ list2)

    groupedLSH.persist(StorageLevel.MEMORY_AND_DISK)






    val candidatesNeighbors: RDD[(VertexId, VertexId)] = groupedLSH.flatMapValues{
      case(listCandidateNeighbors:mutable.LinkedList[(VertexId, SparseVector)])=>generateCouplesFromLinkedList(listCandidateNeighbors)
    }
      .filter{
        case(_,((id1,v1),(id2,v2)))=> 1d-cosine(v1,v2)<=eps
      }.flatMap{
      case (_,((id1,v1),(id2,v2))) =>List((id1,id2),(id2,id1))
    }
      .persist(StorageLevel.MEMORY_AND_DISK)

    //println("DATA COUNT "+data.count())
    //  println("CANDIDATE LSH NEIGHBORS "+candidatesNeighbors.count())
    //  println("CANDIDATE LSH NEIGHBORS DISTINCT "+candidatesNeighbors.distinct().count())

    /**
      * aggrego gli oggetti nello stesso bucket (idbanda-signature)
      */
    val zeroSetElem = collection.mutable.HashSet.empty[Long]


    candidatesNeighbors.aggregateByKey(zeroSetElem)(
      (set, id) => set+=id,
      (set1, set2) => set1 ++ set2)
      .filter{
        case (_, set) => set.size>=minPts
      }

    val filteredneighList =  candidatesNeighbors
      .aggregateByKey(zeroSetElem)(
        (set, id) => set+=id,
        (set1, set2) => set1 ++ set2)
      .filter{
        case (_, set) => set.size>=minPts
      }.flatMap {
      case (idCore, listNeighbor) => listNeighbor map ( neighbor => (idCore, neighbor))
    }.persist(StorageLevel.DISK_ONLY)


    val graph: Graph[Int, Int] = Graph.fromEdgeTuples(filteredneighList, 1,None,StorageLevel.MEMORY_AND_DISK)
    val connectedComponents: VertexRDD[VertexId] = graph.connectedComponents().vertices;

    connectedComponents



  }








  def exacteCosine(a:SparseVector,b:SparseVector):Double={
    1-math.acos(cosine(a,b))/math.Pi
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


  def evaluateLSHModel(lshModel:LSHModel,data: RDD[(VertexId, SparseVector)]): (Double, Double, Double, Double) ={


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
    // implicit val serializer=new Tuple2Serializer[Int,String]
    // implicit val serializer2=new Tuple2Serializer[Long,Iterable[(Int,String)]]

    val indexedInvertedLsh: IndexedRDD[VertexId, String] = IndexedRDD(invertedLsh).cache()

    // Calculate a sum of set bits of XOR'ed bytes
    def hammingDistance(b1:String, b2: String):Double = {
      ((b1 zip b2) count ( x => x._1 != x._2 )).toDouble/b1.length.toDouble
    }





    val cosineSim=data.cartesian(data).filter{
      case((id1,_),(id2,_))=>id1<id2
    }.map {
      case ((id1, v1), (id2, v2)) => {
        //val cosineSim=cosine(v1,v2)
        //if(cosineSim>0.0) Some((id1,id2,1-cosineSim))
        // else None
        (id1,id2,exacteCosine(v1,v2))

      }
    }.collect()
    val errors=cosineSim.map{
      case(ida,idb,cosineDistance)=>{
        val signatureA =indexedInvertedLsh.get(ida).get
        val signatureB=indexedInvertedLsh.get(idb).get
        val hamm=hammingDistance(signatureA,signatureB)
        val approximateCosine: Double =hammingDistance( signatureA,signatureB)
        val error=math.abs((cosineDistance-approximateCosine))/cosineDistance
        println ((ida,idb)+ "COSINE DISTANCE "+cosineDistance+ " approx "+approximateCosine+" ERROR :"+error)
        error
      }
    }


    val avg=errors.reduce(_+_)/errors.length
    val dev=Math.sqrt(errors.map(x=>Math.pow(x-avg,2d)).reduce(_+_)/errors.length)
    println(" AVG ERROR  "+ avg + " MAX ERROR "+errors.max+" STD DEV "+dev)

    (avg,errors.min,errors.max,dev)
    //cosineSim.foreach(println)
  }




  def main(args: Array[String]) {




    //  val c= (1 to 500000).par.map(x=>if(x%2==0) '1' else '0').toString()
    //   print(c.par.map(x=>if(x=='0') false else true).toVector)

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("annotations")
      .set("spark.executor.memory ", "14g")
      .set("spark.local.dir", "/tmp/spark-temp");
    val sc = new SparkContext(sparkConf)




    val minDAte = TweetCollection.findMinMaxValueDate()
    println("min date value" + minDAte)

    val maxDate = new Date(minDAte.getTime + 1000000)
    //val tweetsfirst72H: RDD[(VertexId, Tweet)] = SparkMongoIntegration.getTweetsAsRDDInTimeInterval(sc, minDAte,maxDate)
    val relevantTweets=SparkMongoIntegration.getTweetsAsTupleRDD(sc,None,"onlyRelevantTweets")

    //  val tentweets=tweetsfirst72H.take(10);
    relevantTweets.cache()
    println(" NUMBER Of tweeets" + relevantTweets.count())

    println("finished loading tweets from mongo")
    /**
      * primo passo da fare è generare
      * per ogni tweet vettori di hashingTF
      */
    val sizeDictionary=Math.pow(2, 19).toInt
    val tfidfVectors: RDD[(VertexId, SparseVector)] = generateTfIdfVectors(relevantTweets, sizeDictionary)

    val vectors=generateTfIdfVectors2(relevantTweets, sizeDictionary)

    // Cluster the data into two classes using KMeans
    val numClusters = 500
    val numIterations = 20
    val clusters = KMeans.train(vectors, numClusters, numIterations)
    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(vectors)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    // Save and load model
    clusters.save(sc, "target/kmeansModel")



    val lsh = new LSHWithData(tfidfVectors, sizeDictionary, numHashFunc  =30, numHashTables = 10)
    val lshModel = lsh.run(sc)
    //lshModel.save(sc, "target/relevantTweetsLSH")

    //val lshModel: LSHModel= LSHModel.load(sc, "target/relevantTweetsLSH")

    println("finished loading lsh model")
    val connectedComponents: VertexRDD[VertexId] =clusteringTweets(sc,lshModel,50,0.35)
    val clusteredData: RDD[(VertexId, VertexId)] =
      relevantTweets.leftOuterJoin(connectedComponents)
        .map{
          case(objectId,(instance,Some(clusterId)))=>(clusterId,objectId)
          case(objectId,(instance,None))=>(NOISE,objectId)
        }
    //clusteredData.saveAsTextFile("target/relevantTweetsClustered")
    clusteredData.groupByKey().map(x=>(x._1,x._2.size)).collect().foreach(println)

  }

}

