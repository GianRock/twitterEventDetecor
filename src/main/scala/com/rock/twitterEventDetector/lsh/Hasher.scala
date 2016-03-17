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