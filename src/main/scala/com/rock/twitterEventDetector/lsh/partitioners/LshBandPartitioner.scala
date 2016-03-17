package com.rock.twitterEventDetector.lsh.partitioners

import org.apache.spark.Partitioner

/**
  * Created by rocco on 16/03/16.
  */
class LshBandPartitioner(numParts: Int)  extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = key.toString.toInt%numPartitions
  override def equals(other: Any): Boolean = other match {
    case dnp: LshBandPartitioner =>
      dnp.numPartitions == numPartitions
    case _ =>
      false
  }}