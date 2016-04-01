package com.rock.twitterEventDetector.dbscanScala

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.Map
import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.Iterable
import scala.collection.mutable
import scala.collection.breakOut
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

  /**
    *
    * @param coll
    * @param o
    * @param cbf
    * @tparam T
    * @tparam CC
    * @return
    */
  def mode
  [T, CC[X] <: Seq[X]](coll: CC[T])
                      (implicit o: T => Ordered[T], cbf: CanBuildFrom[Nothing, T, CC[T]])
  : CC[T] = {
    val grouped = coll.groupBy(x => x).mapValues(_.size).toSeq
    val max = grouped.map(_._2).max
    grouped.filter(_._2 == max).map(_._1)(breakOut)
  }
  val NOISE= {
    -1l
  }



  /**
    *
     *@param borderObjects
    * @param clusters
    */
  def assignBorderToCluster(borderObjects:  Map[VertexId, mutable.HashSet[VertexId]], clusters: Map[VertexId, VertexId]): collection.Map[VertexId, VertexId] = {
    borderObjects.map {
        case(idBorder,neighbors)=>
          val clustersIDS: mutable.HashSet[VertexId] =neighbors.flatMap(neighborID=>clusters.get(neighborID))
       //  val modeCluster: mutable.HashSet[VertexId] =mode(clustersIDS)
        val cluster=  if(clustersIDS.isEmpty)
            NOISE
          else
          mode(clustersIDS.toList).head
          (idBorder,cluster)
      }
  }


  def run [T <: Distance[T]](sparkContext: SparkContext, data : RDD[(Long, T)]): RDD[(VertexId, VertexId)] = {


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

    /**
      * archi solo fra core-ids
      */
    val coreSxDX: RDD[(VertexId, VertexId)] = coreObjects.flatMap {
      case (coreId, neighbors) =>
        val coreNeighbors = neighbors.filter(x => broadCastIds.value.contains(x) && coreId<x).toSeq
        coreNeighbors.map(x => (coreId, x))
    }

    val graph: Graph[Int, Int] = Graph.fromEdgeTuples(coreSxDX, 1)
    /*

      contiene è un rdd idogetto-idcluster (che non è altro che l'id più piccolo della componente connessa)
     */
    val connectedComponents: VertexRDD[VertexId] = graph.connectedComponents().vertices

    /**
      *
      */
    val clusters = connectedComponents.collect().toMap
    /**
      *
      */

   val borderCollected: Map[VertexId, mutable.HashSet[VertexId]] = nonCoreObjects.collect().toMap
   val clusteredBorder:Map[VertexId, VertexId] = assignBorderToCluster(borderCollected,clusters)
  //  val borderCLustered: RDD[(VertexId, VertexId)] =None
   // connectedComponents.union(borderCLustered)
   val clusteredBorderRDD = sparkContext.makeRDD[(VertexId, VertexId)](clusteredBorder.toList)
    connectedComponents.union(clusteredBorderRDD)
  }



}
