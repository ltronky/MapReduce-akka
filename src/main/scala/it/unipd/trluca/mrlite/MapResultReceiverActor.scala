package it.unipd.trluca.mrlite

import akka.actor.{ActorLogging, Actor}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class MapResult[K,V](res:Map[K,ArrayBuffer[V]])
case object MapResultGet
case object ResReceived

class MapResultReceiverActor[K, V] extends Actor with ActorLogging {
  var inc:mutable.HashMap[K,ArrayBuffer[V]] = mutable.HashMap.empty


  def receive = {
    case x:MapResult[K,V] =>
      x.res.keys foreach { k =>
        if (inc.contains(k))
          inc(k) ++= x.res(k)
        else
          inc += ((k, x.res(k)))
      }
      sender() ! ResReceived


    case MapResultGet => sender() ! inc.toMap
    case _=> log.info("Messaggio ignorato MapResultReceiverActor")
  }
}
