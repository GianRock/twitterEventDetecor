package com.rock.twitterEventDetector.dbscanScala

/**
  * Created by rocco on 08/02/16.
  */
case class CoordinateInstance(id:Long,x:Double,y:Double) extends Distance[CoordinateInstance]{
  override def distance(that:CoordinateInstance):Double =   Math.sqrt(Math.pow((this.x - that.x), 2) + Math.pow(this.y - that.y, 2))


}
