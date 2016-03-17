package com.rock.twitterEventDetector.lsh.partitioners

import org.apache.spark.Partitioner

/**
  * Created by rocco on 16/03/16.
  */
case class Bucket (band: Int, signature: String)

class BucketPartitioner(numParts: Int)  extends Partitioner {
    override def numPartitions: Int = numParts

    override def getPartition(key: Any): Int = {
      val bucket = key.asInstanceOf[((Int, String))]
      bucket._1
    }

  override def equals(other: Any): Boolean = other match {
    case dnp: BucketPartitioner =>
      dnp.numPartitions == numPartitions
    case _ =>
      false
  }
  
}
