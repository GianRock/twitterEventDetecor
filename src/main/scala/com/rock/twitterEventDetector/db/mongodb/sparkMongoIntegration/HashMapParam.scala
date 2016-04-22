package com.rock.twitterEventDetector.db.mongodb.sparkMongoIntegration

import org.apache.spark.{AccumulableParam, SparkConf}
import scala.collection.mutable
import scala.collection.mutable.HashMap
import org.apache.spark.serializer.JavaSerializer

/*
 * Allows a mutable HashMap[String, Int] to be used as an accumulator in Spark.
 * Whenever we try to put (k, v2) into an accumulator that already contains (k, v1), the result
 * will be a HashMap containing (k, v1 + v2).
 *
 * Would have been nice to extend GrowableAccumulableParam instead of redefining everything, but it's
 * private to the spark package.
 */
class HashMapParam extends AccumulableParam[HashMap[String, Set[Int]], (String, Set[Int])] {
  override def addAccumulator(r: mutable.HashMap[String, Set[Int]], t: (String, Set[Int])): mutable.HashMap[String, Set[Int]] = r+=t

  override def addInPlace(r1: mutable.HashMap[String, Set[Int]], r2: mutable.HashMap[String, Set[Int]])=  r1++=r2

  override def zero(initialValue: mutable.HashMap[String, Set[Int]]): mutable.HashMap[String, Set[Int]] = mutable.HashMap.empty[String, Set[Int]]
}