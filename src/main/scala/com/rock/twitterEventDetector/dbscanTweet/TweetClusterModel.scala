package com.rock.twitterEventDetector.dbscanTweet

import com.rock.twitterEventDetector.lsh.LSHModel
import com.rock.twitterEventDetector.model.Tweets.Tweet
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.util.Saveable
import org.apache.spark.rdd.RDD

/**
  * Created by rocco on 10/03/16.
  */
class TweetClusterModel(assignments:RDD[(Long,Long)]) extends Saveable with Serializable {
  override def save(sc: SparkContext, path: String): Unit = {
    assignments.map(_.productIterator.mkString(",")).saveAsTextFile(path)
  }

  override protected def formatVersion: String = "text"


}
