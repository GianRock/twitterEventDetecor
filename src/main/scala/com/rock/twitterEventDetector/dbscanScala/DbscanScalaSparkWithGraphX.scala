package com.rock.twitterEventDetector.dbscanScala

import com.rock.twitterEventDetector.model.Similarity
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
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
class DbscanScalaSparkWithGraphX [T <: Distance[T]](data : RDD[(Long, T)], executionName:String, minPts: Int =4, eps:Double=1.117 ) extends Serializable {

  val NOISE= {
    -2l
  }

  /**
    *
    * @param sparkContext
    * @param neighRDD
    * @return
    */
  def construct(sparkContext: SparkContext,neighRDD:RDD[(Long,Long)])={
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


    val realNeighs=coreSxDX.union(coreSxnonDX)
    realNeighs
  }


  def run(sparkContext: SparkContext): VertexRDD[VertexId] = {


    data.cache()

    val neighRDD: RDD[(Long, Long)] = data.cartesian(data)
      .filter {case ((idA,_), (idB,_)) => idA < idB }
      .filter {case ((_,a),(_,b)) => a.distance(b)<= eps}
      .flatMap{case ((idA,_), (idB,_))=> List((idA,idB), (idB, idA))}

    // val group: RDD[(Long, Iterable[Long])] = neighRDD.groupByKey();

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


    val realNeighs=coreSxDX.union(coreSxnonDX)

    /*
    val ris: RDD[(VertexId, VertexId)] =  neighRDD.groupByKey().filter(x=>x._2.size>minPts).flatMap {
      case (idCore:Long, listNeighbor:Iterable[Long]) => listNeighbor map{ neighbor=>(idCore,neighbor)

      }
    } */

    /*
    val ris = coresVertex.flatMap {
          case (idCore:Long, listNeighbor:Iterable[Long]) =>
            listNeighbor map{neighbor=>(idCore,neighbor)}
    }*/

   // coresVertex.coalesce(1).map(_._1).saveAsTextFile("cores")
    val graph: Graph[Int, Int] = Graph.fromEdgeTuples(realNeighs, 1)
    val connectedComponents: VertexRDD[VertexId] = graph.connectedComponents().vertices
    connectedComponents
/*
    val clusteredData: RDD[(VertexId, T, VertexId)] =
      data.leftOuterJoin(connectedComponents)
        .map{
          case(objectId,(instance,Some(clusterId)))=>(objectId,instance,clusterId)
          case(objectId,(instance,None))=>(objectId,instance,NOISE)
        }*/
   // clusteredData

  }



}
