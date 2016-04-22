package com.rock.twitterEventDetector.model

/**
  * Created by rocco on 05/04/16.
  */
object Distances {


  /**
    *
    * @param set1
    * @param set2
    * @param corpusSize
    * @tparam T
    */
  def  calculateNormalizedGoogleDistance[T<:Comparable[T]](set1:Set[T], set2:Set[T], corpusSize:Int):Double={

  require(set1.nonEmpty && set2.nonEmpty," set must be not empty")

    val intersectionSet=set1.intersect(set2)

    val(maxCard,minCard)=if(set1.size>set2.size) (set1.size,set2.size) else (set2.size,set1.size)
    /**
      * se l'intersezione è < 0
      */
    val dist: Double = if(intersectionSet.size>0){
      val distance= (Math.log(maxCard) - Math.log(intersectionSet.size)) / (Math.log(corpusSize) - Math.log(minCard))
      if(distance>1) 1.0
      else distance
    }else 1.0
    dist
  }




  def  calculateNormalizedGoogleDistanceInt(set1:Set[Int], set2:Set[Int], corpusSize:Int):Double={

    require(set1.nonEmpty && set2.nonEmpty," set must be not empty")

    val intersectionSet=set1.intersect(set2)

    val(maxCard,minCard)=if(set1.size>set2.size) (set1.size,set2.size) else (set2.size,set1.size)
    /**
      * se l'intersezione è < 0
      */
    val dist: Double = if(intersectionSet.size>0){
      val distance= (Math.log(maxCard) - Math.log(intersectionSet.size)) / (Math.log(corpusSize) - Math.log(minCard))
      if(distance>1) 1.0
      else distance
    }else 1.0
    dist
  }

}
