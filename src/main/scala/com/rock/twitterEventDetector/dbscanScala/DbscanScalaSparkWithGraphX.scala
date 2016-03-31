package com.rock.twitterEventDetector.dbscanScala

import com.rock.twitterEventDetector.model.Similarity
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partition, AccumulableParam, SparkContext}

import scala.collection.{Map, mutable}

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
class DbscanScalaSparkWithGraphX( minPts: Int =4, eps:Double=1.117 ) extends Serializable {

  val NOISE= {
    -2l
  }

  /**
    *
    * @param sparkContext
    * @param neighRDD
    * @return
    */
  def constructEdgesFromSimilarities(sparkContext: SparkContext,neighRDD:RDD[(Long,Long)]): RDD[(VertexId, VertexId)] = {
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
        val coreNeighbors = neighbors.filter(x => broadCastIds.value.contains(x)).toSeq
        coreNeighbors.map(x => (coreId, x))
    }
    val coreSxnonDX =nonCoreObjects.flatMap{
      case(nonCoreId,neighbhors)=>
          val core=neighbhors.toStream.find(x => broadCastIds.value.contains(x))
          core match {
            case Some(idCore)=>Some((idCore,nonCoreId))
            case None=>None
          }
    }
    coreSxDX.union(coreSxnonDX)


  }

  def constructMine(sparkContext: SparkContext,neighRDD:RDD[(Long,Long)]): RDD[(VertexId, VertexId)]={
    val zeroSetElem = collection.mutable.HashSet.empty[Long]


    val coreVertexIds=neighRDD.aggregateByKey(zeroSetElem)(
      (set, id) => set+=id,
      (set1, set2) => set1 ++ set2)
      .filter{
        case (_, set) => set.size>=minPts
      }.map(x=>x._1).collect().toSet



    val broadCastIds=sparkContext.broadcast(coreVertexIds)

    val coreSx=neighRDD.filter{
      case(ida,idb)=>
        broadCastIds.value.contains(ida)
    }

    val coreSxDX=coreSx.filter{
      case(ida,idb)=>
        broadCastIds.value.contains(idb)
    }
    val coreSxnonDX: RDD[(VertexId, VertexId)] =coreSx.filter{
      case(ida,idb)=>
        ! broadCastIds.value.contains(idb)
    }.map{
      case(idCore,idNonCore)=>(idNonCore,idCore)
    }.groupByKey().map{x=>(x._2.toList.head,x._1)}


    coreSxDX.union(coreSxnonDX)

  }
  def run [T <: Distance[T]](sparkContext: SparkContext,data : RDD[(Long, T)]): VertexRDD[VertexId] = {


    data.cache()
    val neighRDD: RDD[(Long, Long)] = data.cartesian(data)
      .filter {case ((idA,_), (idB,_)) => idA < idB }
      .filter {case ((_,a),(_,b)) => a.distance(b)<= eps}
      .flatMap{case ((idA,_), (idB,_))=> List((idA,idB), (idB, idA))}

    val realNeighs=constructEdgesFromSimilarities(sparkContext,neighRDD)
    val graph: Graph[Int, Int] = Graph.fromEdgeTuples(realNeighs, 1)
    val connectedComponents: VertexRDD[VertexId] = graph.connectedComponents().vertices
    connectedComponents


  }



}
