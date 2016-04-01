package com.rock.twitterEventDetector.dbscanScala

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

/**
  * Created by rocco on 23/01/2016.
  */
object mainDBSCAN {

  def main(args: Array[String]) {

    val logFile: String = "t48.txt"

    val conf: SparkConf = new SparkConf().setAppName("Simple Application").setMaster("local[16]").set("spark.executor.memory", "1g")
    //SparkConf conf = new SparkConf().setAppName("Simple Application");
    val sc = new SparkContext(conf)




    val lines: RDD[String] = sc.textFile(logFile)
    //  JavaRDD<Integer> lineLengths = lines.map(s ->

    println(lines.count())
    val coordinateRDD: RDD[(Long, CoordinateInstance)] =lines.map(
      f = line => {
        val parts: Array[String] = line.split("\t")
        val id: Long = parts(0).toLong
        val c: CoordinateInstance = new CoordinateInstance(id.asInstanceOf[java.lang.Long],parts(1).toDouble, parts(2).toDouble)

        (id, c)

      }
    )


    val dbscan=new DbscanScalaSparkWithGraphXAggrawal(20,  0.1)
    val connectedComponents: RDD[(VertexId,VertexId)]=dbscan.run(sc,coordinateRDD)

    val clusteredCoordinates2= coordinateRDD.leftOuterJoin(connectedComponents).map {
      case (id, (coordinate, Some(cluster))) => (id,(coordinate,cluster))
      case (id, (coordinate, None)) =>(id,(coordinate,-1))
    }.sortByKey()

    val clusteredCoordinates= clusteredCoordinates2.sortByKey().map{
      case(id,(coordinate,clusterid))=>id+"\t"+coordinate.x+"\t"+coordinate.y+"\t"+clusterid
    }

    clusteredCoordinates.collect().foreach(println)
    clusteredCoordinates.coalesce(1).saveAsTextFile("t48Mine2NOnDirAgg2")

/*
5
    val g=lines.map(
      line => {
        val parts: Array[String] = line.split("\t")
        val id: Long = parts(0).toLong
        val c: CoordinateInstance = new CoordinateInstance(id.asInstanceOf[java.lang.Long],parts(1).toDouble, parts(2).toDouble)
      val cluster=parts(3)
        (id, (c,cluster))

      }
    )
   val d= g.sortByKey().map{
      case(id,(coordinate,clusterid))=>id+"\t"+coordinate.x+"\t"+coordinate.y+"\t"+clusterid
    }
    d.collect().foreach(println)

    /**
      * val clusterdCoordinates = coordinateRDD.join(connectedComponents).map {
      * case (id, (coordinate, cc)) => (coordinate.getX+","+coordinate.getY+","+ cc)
      * }
*/
      //println(clusterdCoordinates.collect())
    // Print the result
 // println(clusteredCoordinates.collect().mkString("\n"))


    //val clusters=connectedComponents.map(x=>x._2).distinct()
    //clusters.collect().foreach(cluster=>println(cluster))

    //connectedComponents.gr*/
  }

}
