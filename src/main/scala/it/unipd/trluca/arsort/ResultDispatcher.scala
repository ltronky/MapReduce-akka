package it.unipd.trluca.arsort

import akka.actor.{ActorLogging, ActorRef, Actor}
import akka.cluster.Member
import akka.contrib.pattern.Aggregator

import scala.collection.{mutable, SortedSet}
import scala.collection.mutable.ArrayBuffer

case class SendResult[K, V](clusterMembers:SortedSet[Member] ,resArray:Array[mutable.HashMap[K,ArrayBuffer[V]]])

class ResultDispatcher[K, V] extends Actor with Aggregator with ActorLogging {

  val results = ArrayBuffer.empty[Unit]
  var originalSender:ActorRef = null
  var clusterSize:Int = 0

  expectOnce {
    case c:SendResult[K, V] =>
      originalSender = sender()
      clusterSize = c.clusterMembers.size
      var i = 0
      c.clusterMembers foreach { m =>
        context.actorSelection(m.address + ConstStr.NODE_ACT_NAME + "/mrra") ! MapResult(c.resArray(i).toMap)
        i += 1
      }
  }

  val handle = expect {
    case ResReceived =>
      results += ResReceived
      if (results.size >= clusterSize) processResult()
  }

  def processResult() {
    unexpect(handle)
    originalSender ! Done
    context.stop(self)
  }

}
