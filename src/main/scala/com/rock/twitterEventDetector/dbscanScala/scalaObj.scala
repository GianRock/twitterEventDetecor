package com.rock.twitterEventDetector.dbscanScala

import java.io.{FileInputStream, ObjectInputStream, ObjectOutputStream, FileOutputStream}

import com.rock.twitterEventDetector.lsh.Hasher
import com.rock.twitterEventDetector.lsh.partitioners.BucketPartitioner
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partitioner, SparkContext, SparkConf}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD

import scala.Predef
import scala.collection._
import scala.collection.generic.CanBuildFrom
import scala.concurrent.Future


/**
  * Created by rocco on 18/02/16.
  */
object scalaObj {


  /**
    *
    * @param coll
    * @param o
    * @param cbf
    * @tparam T
    * @tparam CC
    * @return
    */
  def mode
  [T, CC[X] <: Seq[X]](coll: CC[T])
                      (implicit o: T => Ordered[T], cbf: CanBuildFrom[Nothing, T, CC[T]])
  : CC[T] = {
    val grouped: scala.Seq[(T, PartitionID)] = coll.groupBy(x => x).mapValues(_.size).toSeq
    val max = grouped.map(_._2).max
    grouped.filter(_._2 == max).map(_._1)(breakOut)
  }



  def main(args: Array[String]) {

  val muta =List(1,2,1,1,1,2,2,2,3,3,3,3,3,3,3,3,3,6,65)
    print(mode(muta))

    /*
      val conf: SparkConf = new SparkSConf().setAppName("Simple Application")
        .setMaster("local[16]")
        .set("spark.executor.memory", "12g")
         .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
       val sc=new SparkContext(conf)
      val lines: RDD[String] = sc.textFile(logFile)

     val edges: RDD[(Long, Long)] = lines.map { x =>
        val parts=x.split("\t")
        ( parts(0).toLong, parts(1).toLong)
      }.partitionBy(new EdgePartitioner(10) )
    edges.persist(StorageLevel.MEMORY_AND_DISK)
     val graph  = Graph.fromEdgeTuples(edges, 1,Some(PartitionStrategy.EdgePartition1D),StorageLevel.MEMORY_AND_DISK_SER,StorageLevel.MEMORY_AND_DISK_SER)




    val conn=  graph.connectedComponents()
       print(conn.vertices.count())

*/
  }


  /**
    * Created by rocco on 17/03/16.
    */
  class EdgePartitioner(numParts: Int)  extends Partitioner {
    override def numPartitions: Int = numParts

    override def getPartition(key: Any): Int = {
      val edge = key.asInstanceOf[Long]
      (edge %numParts).toInt
    }

    override def equals(other: Any): Boolean = other match {
      case dnp: BucketPartitioner =>
        dnp.numPartitions == numPartitions
      case _ =>
        false
    }




  }

}