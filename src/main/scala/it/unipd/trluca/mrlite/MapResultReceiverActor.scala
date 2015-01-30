package it.unipd.trluca.mrlite

import akka.actor.{ActorLogging, Actor}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class MapResult[K,V](k:K, v:ArrayBuffer[V])
case object MapResultGet
case object ResReceived

class MapResultReceiverActor[K, V] extends Actor with ActorLogging {
  var inc:mutable.HashMap[K,ArrayBuffer[V]] = mutable.HashMap.empty

  def receive = {
    case x:MapResult[K,V] =>
      if (inc.contains(x.k))
        inc(x.k) ++= x.v
      else
        inc += ((x.k, x.v))
      sender() ! ResReceived


    case MapResultGet => sender() ! inc.toMap
  }
}
