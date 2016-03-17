package com.rock.twitterEventDetector.lsh

/**
  * Created by maruf on 09/08/15.
  */

import com.rock.twitterEventDetector.lsh.partitioners.BucketPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.IndexedSeq

/** Build LSH model with data RDD. Hash each vector number of hashTable times and stores in a bucket.
  *
  * @param data RDD of sparse vectors with vector Ids. RDD(vec_id, SparseVector)
  * @param m max number of possible elements in a vector
  * @param numHashFunc number of hash functions
  * @param numHashTables number of hashTables.
  *
  * */
class LSH(data : RDD[(Long, SparseVector)] = null, m: Int = 0, numHashFunc : Int = 4, numHashTables : Int = 4) extends Serializable {

  /**
    * this function applies  a list of hashfunctions on a sparsevector in input
    * each hashfunctions consiste of one Vector of [-1,1] and will give one boolean as result
    * so if we have k-functions we will hava a signature made of k-bits
    * this signature will be splitted across b-bands
    * so we will have a list of b elements each of which consist of
    * a pair (idBand,signature)
    * an return a list made of  pairs (string
    * @param data
    * @param hashFunctions
    * @return
    */
  private def hashVector(data: SparseVector, hashFunctions: Broadcast[IndexedSeq[(Int,Hasher)]]): List[(Int, String)] = {
    hashFunctions.value.map(a => (a._1 % numHashTables, a._2.hash(data)))
      .groupBy(_._1)
      .map(x => (x._1, x._2.map(_._2).mkString(""))).toList
  }
  def run(sc: SparkContext) : LSHModel = {


    val hashFunctions: IndexedSeq[( Int,Hasher)] = (0 until numHashFunc * numHashTables).map(i => (i,Hasher(m)))
    val broascastHashFunctions: Broadcast[IndexedSeq[( Int,Hasher)]] = sc.broadcast(hashFunctions)



    //val dataRDD = data.cache()

    val hashTables: RDD[((Int, String), Long)] = data.flatMap {
      case (id, sparseVector) =>
        hashVector(sparseVector, broascastHashFunctions).map((_,id))
    }.partitionBy(new BucketPartitioner(numHashTables)).cache()


     new LSHModel(m,numHashFunc,numHashTables,hashFunctions,hashTables)


  }




}
