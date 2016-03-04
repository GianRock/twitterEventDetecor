package com.rock.twitterEventDetector.lsh

/**
 * Created by maruf on 09/08/15.
 */

import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/** Build LSH model with data RDD. Hash each vector number of hashTable times and stores in a bucket.
  *
  * @param data RDD of sparse vectors with vector Ids. RDD(vec_id, SparseVector)
  * @param m max number of possible elements in a vector
  * @param numHashFunc number of hash functions
  * @param numHashTables number of hashTables.
  *
  * */
class LSH(data : RDD[Tuple2[Long, SparseVector]] = null, m: Int = 0, numHashFunc : Int = 4, numHashTables : Int = 4) extends Serializable {

    def run() : LSHModel = {

    //create a new model object
    val model = new LSHModel(m, numHashFunc, numHashTables)

    val dataRDD = data.persist(StorageLevel.MEMORY_AND_DISK_2)

      //compute hash keys for each vector
    // - hash each vector numHashFunc times
    // - concat each hash value to create a hash key
    // - position hashTable id hash keys and vector id into a new RDD.
    // - creates RDD of ((hashTable#, hash_key), vec_id) tuples.




     val c: RDD[(List[(Int, Int)], Long)] =dataRDD
      .map(v => (model.hashFunctions.map(h => (h._1.hash(v._2), h._2 % numHashTables)), v._1))


      val d: RDD[((Int, Long), Iterable[Int])] =c.map(x => x._1.map(a => ((a._2, x._2), a._1)))
      .flatMap(a => a).groupByKey()

      val e: RDD[((Int, String), Long)] =d.map(x => ((x._1._1, x._2.mkString("")), x._1._2))

     //.map(x => (x._1._2, (x._1._1, x._2.mkString(""))



        model.hashTables =e

    model

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


}
