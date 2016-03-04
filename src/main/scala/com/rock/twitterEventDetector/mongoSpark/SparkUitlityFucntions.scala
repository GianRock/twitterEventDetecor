package com.rock.twitterEventDetector.mongoSpark

/**
  * Created by rocco on 03/03/16.
  */
object SparkUitlityFucntions {


  def createListCombiner[A](c:A): List[A] ={
    List(c)
  }
  def mergeListValue[A](acc:List[A],c:A): List[A]={
    c::acc
  }
  def mergeListCombiners[A](acc1:List[A],acc2:List[A]): List[A]={
    acc1++acc2
  }

}
