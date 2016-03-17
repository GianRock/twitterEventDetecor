package com.rock.twitterEventDetector.dbscanTweet

import com.rock.twitterEventDetector.lsh.LSHModel
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.util.Saveable
import org.apache.spark.rdd.RDD

/**
  * Created by rocco on 10/03/16.
  */
class TweetClusterModel(lSHModel: LSHModel,neighborsIds:RDD[(Long,Long)],clusterdData:RDD[(Long,Long)]) extends  Saveable {
  override def save(sc: SparkContext, path: String): Unit = {
    clusterdData.map(_.productIterator.mkString(",")).saveAsTextFile(path)
  }

  override protected def formatVersion: String = "text"
}
