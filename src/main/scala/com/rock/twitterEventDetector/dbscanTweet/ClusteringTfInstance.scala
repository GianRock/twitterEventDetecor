package com.rock.twitterEventDetector.dbscanTweet

import com.rock.twitterEventDetector.model.Tweets.Tweet
import org.apache.spark.mllib.linalg.SparseVector

/**
  * Created by rocco on 19/03/16.
  */
   abstract class ClusteringTfIdfInstance(tfIdfVector:SparseVector) {


  /**
    * default cosine similarity
    * subclasses could override this method in order to provide
    * theri own similarites func


  def similarity(that:ClusteringTfIdfInstance): Double ={
      Distances.cosine(this.tfIdfVector,that.tfIdfVector)
     }
    */
}
    class TweetClusteringInstance( tfIdfVector:SparseVector,tweet: Tweet) extends ClusteringTfIdfInstance(tfIdfVector){

 }
