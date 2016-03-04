package com.rock.twitterEventDetector.lsh
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
 import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by rocco on 03/03/16.
  */
class IndexedRDDLshModel(lshModel: LSHModel) {

  def createIndexedRDDStructures(lshModel: LSHModel) ={

    val zero = collection.mutable.Set.empty[String]
    val zeroLong = collection.mutable.Set.empty[Long]

    val cs: RDD[(Long, mutable.Set[String])] =lshModel.hashTables.map{
      case(hashkey,id)=>(id,hashkey._1+"-"+hashkey._2)
    }.aggregateByKey(zero)(
      (set, v) => set += v,
      (set1, set2) => set1 ++= set2)


    val indexedInvertedLsh: IndexedRDD[Long, mutable.Set[String]] =IndexedRDD(cs).cache()


    val mergedSignatures: RDD[(String, mutable.Set[Long])] =lshModel.hashTables.map(x=>(x._1._1.toString+"-"+x._1._2,x._2)).aggregateByKey(zeroLong)(
      (set, v) => set += v,
      (set1, set2) => set1 ++= set2)
    /**e
      * creo un indexRDD
      * che per ogni ad ogni (banda,signature) associa
      * la lista degli id dei documenti che ricadono
      * in tale bucket
      */
    val indexedHashTable: IndexedRDD[String, mutable.Set[VertexId]] =
      IndexedRDD(mergedSignatures).cache()
    (indexedInvertedLsh,indexedHashTable)

  }

  val (indexedInvertedLsh,indexedHashTables) = createIndexedRDDStructures(lshModel)




  /**
    *  this function retrives  all the candidate neighbors of the document from the lsh model,
    *  with the id supplied.
    *
    *  all the id of documents who lie in the same bucket
    *

    * @param idQuery
    * @return
    */
  def getCandidateListFromIndexedRDD
  ( idQuery:Long )={
    val signatures: mutable.Set[String] =this.indexedInvertedLsh.get(idQuery).get
    val candidateList: List[Long] =signatures.flatMap {
      case (signature:String) =>this.indexedHashTables.get(signature).filterNot(_==idQuery).get.toList
    }.toList
    candidateList.toList
  }







}
