package com.rock.twitterEventDetector.LshWithAllData

import java.util.Date

import com.rock.twitterEventDetector.model.Model.DbpediaResource
import org.apache.spark.mllib.linalg.SparseVector

/**
  * Created by rocco on 01/04/16.
  */
abstract class LshInstance (tfIdfVector:SparseVector) extends Serializable{
  def gettfIdfVector(): SparseVector ={
    this.tfIdfVector
  }
}
case class AnntoatedTweetLshInstance(tfIdfVector:SparseVector,createdAt:Date,dbpediaResources:Set[DbpediaResource]) extends LshInstance(tfIdfVector)