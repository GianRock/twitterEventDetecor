package com.rock.twitterEventDetector.nlp

import scala.collection.immutable.IndexedSeq
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}

/**
  * Created by rocco on 15/02/16.
  */
object FutExample {
  def maincc(args: Array[String]) {
    def sleep(millis: Long) = {
      Thread.sleep(millis)
    }
    // Busy work ;)
    def doWork(index: Int) = {
     // sleep((math.random * 1000).toLong)
      index
    }
   val c: IndexedSeq[Unit] = (1 to 500000) map { index =>
      val future = Future {
        doWork(index)
      }

      future.onComplete{
        case Success(answer:Int)=>println(s"Success! returned: $answer")

        case Failure(th:Throwable)=> println(s"FAILURE! returned: $th")

      }

    }
    sleep(1000) // Wait long enough for the "work" to finish.
    println("Finito!")
  }
}


