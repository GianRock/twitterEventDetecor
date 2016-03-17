package com.rock.twitterEventDetector.utils

import scala.annotation.tailrec
import scala.collection.mutable

/**
  * Created by rocco on 04/03/16.
  */
object UtilsFunctions {


  /**
    * this func given a list of objects
    * generate all the  the possible couples
    *
    * @param list
    * @tparam A
    * @return
    */
  def generateCouplesFromList[A](list:List[A]): List[(A, A)]={
    @tailrec
    def generateCoupleTailRec[A](list:List[A], acc: List[(A,A)]):List[(A,A)]={
      list match {
        case head::Nil=> acc
        case head :: tail =>
          val couples=tail.map(x=>(head,x))
          // val couples = List(head).zipAll(tail, head, 0)
          generateCoupleTailRec(tail, acc ++ couples)
      }
    }

    generateCoupleTailRec(list, List())
  }




  /**
    * this func given a list of objects
    * generate all the  the possible couples
    *
    * @param list
    * @tparam A
    * @return
    */
  def generateCouplesFromLinkedList[A](list:mutable.LinkedList[A]): List[(A, A)]={
    @tailrec
    def generateCoupleTailRec[A](list:mutable.LinkedList[A], acc: List[(A,A)]):List[(A,A)]={
      list.size ==1 match {
        case true => acc
        case false =>
          val couples = list.tail.map(x=>(list.head,x))
          generateCoupleTailRec(list.tail, acc ++ couples)
      }
    }

    generateCoupleTailRec(list, List())
  }


}
