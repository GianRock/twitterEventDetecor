package com.rock.twitterEventDetector.dbscanTweet

import com.rock.twitterEventDetector.lsh.Hasher
import com.rock.twitterEventDetector.lsh.partitioners.LshBandPartitioner
import org.apache.spark.mllib.linalg.{Vector, Vectors, SparseVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.immutable.IndexedSeq

/**
  * Created by rocco on 16/03/16.
  */
object SerialExample {
  class Person(val name: String,val vector:SparseVector) extends Serializable

  def main(args: Array[String]) {

    val size=Math.pow(2,19).toInt
    val outputPath = "target/hashersExamplePartitionedSer"

    val conf = new SparkConf().setMaster("local").setAppName("kryoexample")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    //create some dummy data
    val v=Vectors.sparse(size, Array(0, 2,10), Array(1.0, 3.0,0.11))

    val numBand=30
    val signature="100001210"
    val id=15L

    val prova: IndexedSeq[(Int, Hasher)] = (1 to 10000 map(value=>value)).map(x=>(x,Hasher(size)))
    val rddM= sc.makeRDD(1 to 10000 map(value=>value)).map(x=>(x,Hasher(size))).partitionBy(new LshBandPartitioner(numBand))
    rddM.saveAsObjectFile(outputPath)
    //val rddModel=  1 to 10 map (value=>(((band,signature),(value.toLong,v))))


   // val hashers=1 to 400 map (value=>((value%numBand,Hasher(size))))
    //val hashersRDD=sc.makeRDD(hashers).partitionBy(new LshBandPartitioner(numBand))
   // hashersRDD.saveAsObjectFile(outputPath)
    val rdd =sc.objectFile[(Int,Hasher)] (outputPath)

   // rdd.lookup(0).foreach(x=>println(x.r))
    rdd.take(3).foreach(x=>println(x._1,x._2.r))

/*

    val personList = 1 to 10000000 map (value => new Person(value.toString ,v.toSparse))


    val personRDD = sc.makeRDD(personList)
    val lshModelRDD: RDD[((Int, String), (Long, Vector))] =sc.makeRDD(rddModel)
    //lshModelRDD.saveAsObjectFile(outputPath)
    val rdd =sc.objectFile[((Int,String),(Long,SparseVector))] (outputPath)*/

    // println(rdd.map(b =>b._2).take(100).toList)
  }

}
