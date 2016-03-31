package com.rock.twitterEventDetector.dbscanScala

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
  * Created by rocco on 23/01/2016.
  * this class expolits GraphX in order to cluster data with DBSCAN.
  * More precisely, given an rdd  of any types of objects
  * for whome is defined some distance measures,
  * it firsts compute a new RDD mad of all the couples (a,b) whose distance is below a certain treshold Eps.
  * this Rdd can be viewed as a DG where the vertex are the objects and the edges will connect all the couples
  * (a,b) where d(a,b)<Eps
  *
  * If we use the function ConnectedComponents on that Dag, it will return
  * the clusters.
  *
  *
  *
  */
class DbscanScalaSparkWithGraphXAggrawal(minPts: Int =4, eps:Double=1.117 ) extends Serializable {

  val NOISE= {
    -2l
  }

  /**
    *
    * @param nonCoreObjects
    * @param broadcast
    */
  def assignBorderToCluster(nonCoreObjects: RDD[(VertexId, mutable.HashSet[VertexId])], broadcast: Broadcast[Array[VertexId]]) = {
    nonCoreObjects.flatMap {
      case(nonCoreId,nonCoreIdSet)=>
        val coreOption=nonCoreIdSet.toStream.find(x => broadcast.value.contains(x))
        coreOption match {
          case Some(idCore)=>Some((nonCoreId,idCore))
          case None=>None
        }
    }
  }



  def run [T <: Distance[T]](sparkContext: SparkContext,data : RDD[(Long, T)]): RDD[(VertexId, VertexId)] = {


    data.cache()
    val neighRDD: RDD[(Long, Long)] = data.cartesian(data)
      .filter {case ((idA,_), (idB,_)) => idA < idB }
      .filter {case ((_,a),(_,b)) => a.distance(b)<= eps}
      .flatMap{case ((idA,_), (idB,_))=> List((idA,idB), (idB, idA))}

    val zeroSetElem = collection.mutable.HashSet.empty[Long]



    val grouped = neighRDD.aggregateByKey(zeroSetElem)(
      (set, id) => set += id,
      (set1, set2) => set1 ++ set2).cache(
    )//.persist(StorageLevel.MEMORY_AND_DISK)



    val coreObjects = grouped.filter {
      case (_, set) => set.size >= minPts
    }.persist(StorageLevel.MEMORY_AND_DISK)
    val nonCoreObjects = grouped.filter {
      case (_, set) => set.size < minPts
    }

    val coreIds = coreObjects.keys.collect().toSet
    val broadCastIds = sparkContext.broadcast(coreIds)


    val coreSxDX: RDD[(VertexId, VertexId)] = coreObjects.flatMap {
      case (coreId, neighbors) =>
        val coreNeighbors = neighbors.filter(x => broadCastIds.value.contains(x) && coreId<x).toSeq
        coreNeighbors.map(x => (coreId, x))
    }

    val graph: Graph[Int, Int] = Graph.fromEdgeTuples(coreSxDX, 1)
    val connectedComponents: VertexRDD[VertexId] = graph.connectedComponents().vertices

    val clusters: Array[VertexId] = connectedComponents.values.collect()
    val borderCLustered: RDD[(VertexId, VertexId)] =assignBorderToCluster(nonCoreObjects,sparkContext.broadcast(clusters))
    connectedComponents.union(borderCLustered)


  }



}
