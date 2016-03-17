package com.rock.twitterEventDetector.lsh

/**
  * Created by maytekin on 06.08.2015.
  */

import akka.io.Udp.SO.Broadcast
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import scala.collection.immutable.{IndexedSeq, BitSet}
import scala.collection.mutable.ListBuffer
import org.apache.spark.mllib.util.{Saveable}

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


/** Create LSH model for maximum m number of elements in each vector.
  *
  * @param m max number of possible elements in a vector
  * @param numHashFunc number of hash functions
  * @param numHashTables number of hashTables.
  *
  * */
class LSHModelWithData(val m: Int, val numHashFunc : Int, val numHashTables: Int,val hashFunctions: Seq[(Hasher, Int)], var hashTables: RDD[((Int, String), (Long,SparseVector))])
  extends Serializable with Saveable {




  /** hash a sinasgle vector against an existing model and return the candidate buckets */
  def filter(data: SparseVector, model: LSHModel, itemID: Long): RDD[Long] = {
    val hashKey = hashFunctions.map(h => h._1.hash(data)).mkString("")
    hashTables.filter(x => x._1._2 == hashKey).map(a => a._2._1)
  }

  /** creates hashValue for each hashTable.*/
  def hashValue(data: SparseVector): List[(Int, String)] =
    hashFunctions.map(a => (a._2 % numHashTables, a._1.hash(data)))
      .groupBy(_._1)
      .map(x => (x._1, x._2.map(_._2).mkString(""))).toList

  /** returns candidate set for given vector id.*/
  def getCandidates(vId: Long): RDD[(Long, SparseVector)] = {
    val buckets = hashTables.filter(x => x._2 == vId).map(x => x._1).distinct().collect()
    hashTables.filter(x => buckets contains x._1).map(x => x._2).filter(x => x != vId)
  }

  /** returns candidate set for given vector.*/
  def getCandidates(v: SparseVector): RDD[Long] = {
    val hashVal = hashValue(v)
    hashTables.filter(x => hashVal contains x._1).map(x => x._2._1)
  }

  /** adds a new sparse vector with vector Id: vId to the model. */
  def add (vId: Long, v: SparseVector, sc: SparkContext): LSHModelWithData = {
    val newRDD = sc.parallelize(hashValue(v).map(a => (a, (vId,v))))
    hashTables ++ newRDD
    this
  }


  /** remove sparse vector with vector Id: vId from the model. */
  def remove (vId: Long, sc: SparkContext): LSHModelWithData = {
    hashTables =  hashTables.filter(x => x._2 != vId)
    this
  }
  def remove (expiderObjects: org.apache.spark.broadcast.Broadcast[Set[Long]]): LSHModelWithData = {
    hashTables =  hashTables.filter(x =>expiderObjects.value.contains(x._2._1)==0)
    this
  }

  override def save(sc: SparkContext, path: String): Unit =
    LSHModelWithData.SaveLoadV0_0_1.save(sc, this, path)

  override protected def formatVersion: String = "0.0.1"

}

object LSHModelWithData {

  def load(sc: SparkContext, path: String): LSHModelWithData = {
    LSHModelWithData.SaveLoadV0_0_1.load(sc, path)
  }

  private [lsh] object SaveLoadV0_0_1 {

    private val thisFormatVersion = "0.0.1"
    private val thisClassName = this.getClass.getName()

    def save(sc: SparkContext, model: LSHModelWithData, path: String): Unit = {

      val metadata =
        compact(render(("class" -> thisClassName) ~ ("version" -> thisFormatVersion) ~ ("numHashTables" -> model.numHashTables)
          ~ ("numHashFunc" -> model.numHashFunc)))

      //save metadata info


      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

      /*
      //save hash functions as (hashTableId, randomVector)
      val textHasFunctions: RDD[String] =sc.parallelize(model.hashFunctions.map {
        case(hasher,band)=>band+"-"+hasher.r.map{x=>if(x) '0' else '1'}.mkString("")
      })
*/

      //textHasFunctions .saveAsTextFile(Loader.hasherPath(path))

      //save data as (hashTableId#, hashValue, vectorId,sparsevector)
      /*
      model.hashTables
        .map(x => (x._1._1, x._1._2, x._2))
        .map(_.productIterator.mkString(","))
        .saveAsTextFile(Loader.dataPath(path))*/


     sc.makeRDD[(Hasher,Int)]( model.hashFunctions).saveAsObjectFile(Loader.hasherPath(path))
      model.hashTables.saveAsObjectFile(Loader.dataPath(path))

    }

    def load(sc: SparkContext, path: String): LSHModelWithData = {

      implicit val formats = DefaultFormats
      val (className, formatVersion, numBands,numHashFunc,metadata) = MYLoader.loadMetadata(sc, path)
     // assert(className == thisClassName)
      //assert(formatVersion == thisFormatVersion)
      val hashTables =sc.objectFile[((Int,String),(Long,SparseVector))] (Loader.dataPath(path)).partitionBy(new BucketPartitioner(numBands))
      val hashers=sc.objectFile[(Hasher,Int)](Loader.hasherPath(path)).collect()

    /*
      val numBands = hashTables.map(x => x._1._1).distinct.count()
      val numHashFunc = hashers.size / numBands*/

      /*
      sc.textFile(Loader.dataPath(path))
      .map(x => x.split(","))
      .map(x => ((x(0).toInt, x(1)), x(2).toLong) )*/


      //  val hashers=parts.map(x => (Hasher(x._2), x._1.toInt)).collect().toList
  /*

      val hashers = sc.textFile(Loader.hasherPath(path))
        .map(a => a.split("-"))
        .map(x => ( Hasher(x.apply(1)), x.head.toInt)).collect()*/
      // println(hashers.foreach(x=>println(x._1.r+"  band "+x._2)))



      //Validate loaded data
      //check size of data
     // assert(hashTables.count != 0, s"Loaded hashTable data is empty")
      //check size of hash functions
     // assert(hashers.size != 0, s"Loaded hasher data is empty")

      //check hashValue size. Should be equal to numHashFunc
      //assert(hashTables.map(x => x._1._2).filter(x => x.size != numHashFunc).collect().size == 0,
      //  s"hashValues in data does not match with hash functions")

      //create model
      val model = new LSHModelWithData(0, numHashFunc, numBands,hashers,hashTables)


      model
    }
  }
}


/** Helper functions for save/load data from mllib package.
  * TODO: Remove and use Loader functions from mllib. */
private[lsh] object MYLoader {

  /** Returns URI for path/data using the Hadoop filesystem */
  def dataPath(path: String): String = new Path(path, "data").toUri.toString

  /** Returns URI for path/metadata using the Hadoop filesystem */
  def metadataPath(path: String): String = new Path(path, "metadata").toUri.toString

  /** Returns URI for path/metadata using the Hadoop filesystem */
  def hasherPath(path: String): String = new Path(path, "hasher").toUri.toString

  /**
    * Load metadata from the given path.
    *
    * @return (class name, version, metadata)
    */
  def loadMetadata(sc: SparkContext, path: String): (String, String, Int, Int, JValue) = {
    implicit val formats = DefaultFormats
    val metadata = parse(sc.textFile(metadataPath(path)).first())
    val clazz = (metadata \ "class").extract[String]
    val version = (metadata \ "version").extract[String]
    val numHashTables = (metadata \ "numHashTables").values.asInstanceOf[BigInt].toInt
    val numHashFuncs = (metadata \ "numHashFunc").values.asInstanceOf[BigInt].toInt
    (clazz, version, numHashTables,numHashFuncs,metadata)
  }

}