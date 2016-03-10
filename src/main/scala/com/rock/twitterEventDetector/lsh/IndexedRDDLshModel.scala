package com.rock.twitterEventDetector.lsh

import com.rock.twitterEventDetector.model.Tweets.Tweet
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
 import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
import org.apache.spark.graphx._
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD

import scala.Predef
import scala.collection.immutable.Iterable
import scala.collection.{Map, mutable}

/**
  * Created by rocco on 03/03/16.
  */
class IndexedRDDLshModel(lshModel: LSHModel) extends  Serializable{
  val (indexedLSHSignatures,indexedHashTables) = createIndexedRDDStructures(lshModel)

  def createIndexedRDDStructures(lshModel: LSHModel) = {

    val zero = collection.mutable.Set.empty[String]
    val zeroLong = collection.mutable.Set.empty[Long]

    val cs: RDD[(Long, mutable.Set[String])] = lshModel.hashTables.map{
      case(hashkey,id)=>(id,hashkey._1+"-"+hashkey._2)
    }.aggregateByKey(zero)(
      (set, v) => set += v,
      (set1, set2) => set1 ++= set2)

    val indexedLSHSignatures = cs.collectAsMap()
  //  val indexedInvertedLsh: IndexedRDD[Long, mutable.Set[String]] =IndexedRDD(cs).cache()

//TODO extract mergedSignatures using cs
    val mergedSignatures: RDD[(String, mutable.Set[Long])] =
      lshModel.hashTables.map( x => (x._1._1.toString+"-"+x._1._2,x._2))
        .aggregateByKey(zeroLong)(
      (set, v) => set += v,
      (set1, set2) => set1 ++= set2)

    /**e
      * creo un indexRDD
      * che per ogni ad ogni (banda,signature) associa
      * la lista degli id dei documenti che ricadono
      * in tale bucket
      */
    val indexedHashTable: IndexedRDD[String, mutable.Set[VertexId]] = IndexedRDD(mergedSignatures).cache()

    (indexedLSHSignatures, indexedHashTable)

  }





  /**
    *  this function retrives  all the candidate neighbors of the document from the lsh model,
    *  with the id supplied.
    *
    *  all the id of documents who lie in the same bucket
    *

    * @param idQuery
    * @return
    */
  def getCandidateListFromIndexedRDD(idQuery: Long ) = {
    val signatures =
      this.indexedLSHSignatures.get(idQuery).get.toArray
    val d: Iterable[VertexId] = this.indexedHashTables.multiget(signatures)
      .flatMap( x => x._2.filter(i => i>idQuery))
    d
  }








}
