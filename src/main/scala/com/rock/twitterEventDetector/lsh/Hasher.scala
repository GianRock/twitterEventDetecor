package com.rock.twitterEventDetector.lsh

import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.util.random

import scala.annotation.tailrec
import scala.collection.immutable.{BitSet, IndexedSeq}
import scala.collection.mutable.ArrayBuffer
import scala.util.Random


/**
  * Simple hashing function implements random hyperplane based hash functions described in
  * http://www.cs.princeton.edu/courses/archive/spring04/cos598B/bib/CharikarEstim.pdf
  * r is a random vector. Hash function h_r(u) operates as follows:
  * if r.u < 0 //dot product of two vectors
  *    h_r(u) = 0
  *  else
  *    h_r(u) = 1
  **/
class Hasher(val r: Vector[Boolean]) extends Serializable {
  /** hash SparseVector v with random vector r */
  def hash(u : SparseVector) : Int = {
    val rVec: Array[Boolean] = u.indices.map(i => r(i))
    val hashVal = (rVec zip u.values).map(_tuple => if(_tuple._1) _tuple._2 else -_tuple._2).sum
    if (hashVal > 0) 1 else 0
  }

}

object Hasher extends Serializable{

  def fromString(stringSerialized: String): Vector[Boolean] ={
    //println(stringSerialized)
    val vector=stringSerialized.par.map(x=>if(x=='0') false else true).toVector
    //println(vector)
    vector
  }

  def apply(stringSerialized:String)={
    new Hasher(fromString(stringSerialized))

  }

  /** create a new instance providing size of the random vector Array [Double] */
  def apply (size: Int, seed: Long = System.nanoTime) = new Hasher(r(size, seed))

  /** create a random vector whose whose components are -1 and +1 */
  def r(size: Int, seed: Long): Vector[Boolean] = {
    val rnd = new Random(seed)
    (0 until  size).map{
      _=> if (rnd.nextGaussian() < 0) false else true

    }.toVector
  }
}


//
///**
//  * Simple hashing function implements random hyperplane based hash functions described in
//  * http://www.cs.princeton.edu/courses/archive/spring04/cos598B/bib/CharikarEstim.pdf
//  * r is a random vector. Hash function h_r(u) operates as follows:
//  * if r.u < 0 //dot product of two vectors
//  *    h_r(u) = 0
//  *  else
//  *    h_r(u) = 1
//  **/
//class Hasher(val r: Vector[Double]) extends Serializable {
//  /** hash SparseVector v with random vector r */
//  def hash(u : SparseVector) : Int = {
//    val rVec: Array[Double] = u.indices.map(i => r(i))
//    val hashVal = (rVec zip u.values).map(_tuple => _tuple._1 * _tuple._2).sum
//    if (hashVal > 0) 1 else 0
//  }
//
//}
//
//object Hasher extends Serializable{
//
//  def fromString(stringSerialized: String): Vector[Boolean] ={
//    //println(stringSerialized)
//    val vector=stringSerialized.par.map(x=>if(x=='0') false else true).toVector
//    //println(vector)
//    vector
//  }
///*
//  def apply(stringSerialized:String)={
//   new Hasher(fromString(stringSerialized))
//
//  }*/
//
//  /** create a new instance providing size of the random vector Array [Double] */
//  def apply (size: Int, seed: Long = System.nanoTime) = new Hasher(r(size, seed))
//
//  /** create a random vector whose whose components are -1 and +1 */
//  def r(size: Int, seed: Long): Vector[Double] = {
//    val rnd = new Random()
//    val hashVec: Vector[Double] =(0 until  size).map{
//      _=> rnd.nextGaussian()
//
//    }.toVector
//    val norm =Math.sqrt(hashVec.map(x => Math.pow(x, 2)).sum)
//    hashVec.map(x=>x/norm)
//
//  }

/*




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


  def cr(size: Int, seed: Long=System.nanoTime()): Vector[Double] = {
    val rnd = new Random(seed)
   val vec= (0 until  size).map{
      _=> rnd.nextGaussian()

    }.toVector
    val norm=vec.map(x => Math.pow(x, 2)).sum
    vec.map(x=>x/norm)
    vec

  }

  def dotProductInt(a:Vector[Int],b:Vector[Int])={
   val g =a.zip(b).filter{
     case(a,b)=>a==b
   }.size
   g.toDouble/a.size.toDouble
 }
  def dotProduct(a:Vector[Double],b:Vector[Double]): Double ={
    val dotProduct=a.zip(b).map{
      case(a,b)=>a*b
    }.sum
    dotProduct
  }


  def cosine(a: Vector[Double], b: Vector[Double]): Double = {


     val magnitudeA = a.map(x => Math.pow(x, 2)).sum
    val magnitudeB = b.map(x => Math.pow(x, 2)).sum
    val dotProduct=a.zip(b).map{
      case(a,b)=>a*b
    }.sum

      val cosine=dotProduct/ (Math.sqrt(magnitudeA) * Math.sqrt(magnitudeB))

    return  cosine
  }
  def main(args: Array[String]) {


    var avgMedio = List.empty[Double]

    Range(1, 100).foreach { _ =>
      val n = Math.pow(2, 10).toInt

      val hashes: List[Vector[Double]] = (1 to 10).map(x =>cr(n)).toList


      val lista = generateCouplesFromList(hashes)
      val total: Double = lista.map(x => cosine(x._1, x._2)).reduce(_ + _)

      val avg = total / lista.size.toDouble
      println("AVG " + avg)

      avgMedio = avgMedio :+ avg

   //  hashes.map(x => x.sum/x.size).foreach(x=>println("MEDIA VETTORE HASH"+x))
    }

    val sum = avgMedio.sum
    val total = sum/avgMedio.size
    println("AVG tot " + total)


  }
*/




/*
class BitSetHasher(val r: BitSet) extends Serializable {
  /** hash SparseVector v with random vector r */
  def hash(u : SparseVector) : Int = {
    val rVec: Array[Boolean] = u.indices.map(i => r(i))
    val hashVal = (rVec zip u.values).map(_tuple => if(_tuple._1) _tuple._2 else -_tuple._2).sum
    if (hashVal > 0) 1 else 0
  }

}
object BitMapHasher {

  /** create a new instance providing size of the random vector Array [Double] */
  def apply (size: Int, seed: Long = System.nanoTime) = new BitSetHasher(r(size, seed))

  /** create a random vector whose whose components are -1 and +1 */
  def r(size: Int, seed: Long) : BitSet  = {

    @tailrec
    def ra(size: Int, rnd:Random,count:Int,acc:BitSet) : BitSet = {

      if(count>=size) acc
      else
      {

        ra(size,rnd,count+1, if(rnd.nextGaussian()<0) acc else acc+count)

      }
    }
    val rnd = new Random(seed)
    val c=ra(size,rnd,0,BitSet.empty)
    println(c.size)
    c
    /*
    for (i <- 0 until size)
      buf += (if (rnd.nextGaussian() < 0) -1 else 1)
    buf.toArray*/
  }




  @tailrec
  def ra(size: Int, seed: Long,count:Int,acc:Vector[Double]) : Vector[Double] = {

    if(count>=size) acc
    else
    {
      val rnd = new Random(seed)
      ra(size,seed,count+1,acc:+(if (rnd.nextGaussian() < 0) -1d else 1d))

    }

  }

}
class BooleanHasher(val r: Array[Boolean]) extends Serializable {

  /** hash SparseVector v with random vector r */
  def hash(u : SparseVector) : Int = {
    val rVec: Array[Boolean] = u.indices.map(i => r(i))
    val hashVal = (rVec zip u.values).map(_tuple => if(_tuple._1) _tuple._2 else -_tuple._2 ).sum
    if (hashVal > 0) 1 else 0
  }

}

object BooleanHasher {

  /** create a new instance providing size of the random vector Array [Double] */
  def apply (size: Int, seed: Long = System.nanoTime) = new BooleanHasher(r(size, seed))

  /** create a random vector whose whose components are -1 and +1 */
  def r(size: Int, seed: Long) : Array[Boolean] = {
    val buf = new ArrayBuffer[Boolean]
    val rnd = new Random(seed)
    for (i <- 0 until size)
      buf += (if (rnd.nextGaussian() < 0) false else true)
    buf.toArray
  }

}


class VectorHasher(val r:Vector[Double])extends Serializable {
  def hash(u : SparseVector) : Int = {
    val rVec=u.indices.map(i => r(i))
    val hashVal = (rVec zip u.values).map(_tuple => _tuple._1 * _tuple._2).sum
    if (hashVal > 0) 1 else 0
  }
}
object VectorHasher{

  /** create a random vector whose whose components are -1 and +1 */
  def r(size: Int, seed: Long) : Vector[Double] = {
    val rnd = new Random(seed)
    (0 until  size).map{
      _=> if (rnd.nextGaussian() < 0) -1d else 1d
    }.toVector


  }

}*/