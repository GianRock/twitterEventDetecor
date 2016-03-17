package com.rock.twitterEventDetector.lsh.partitioners
import org.apache.spark.Partitioner

/**
  * Created by rocco on 17/03/16.
  */
class HashFuncPartitioner(numParts: Int)  extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    val bucket = key.asInstanceOf[Int]
    bucket%numParts
  }

  override def equals(other: Any): Boolean = other match {
    case dnp: BucketPartitioner =>
      dnp.numPartitions == numPartitions
    case _ =>
      false
  }




}
