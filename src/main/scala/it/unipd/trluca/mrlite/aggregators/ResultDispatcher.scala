package it.unipd.trluca.mrlite.aggregators

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.cluster.Member
import akka.contrib.pattern.Aggregator
import it.unipd.trluca.mrlite.{Consts, MapResult, ResReceived}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{SortedSet, mutable}

case class SendResult[K, V](clusterMembers:SortedSet[Member] ,resArray:Array[mutable.HashMap[K,ArrayBuffer[V]]])

class ResultDispatcher[K, V] extends Actor with Aggregator with ActorLogging {

  val results = ArrayBuffer.empty[Unit]
  var originalSender:ActorRef = null
  var expectedResultSize:Int = 0

  expectOnce {
    case c:SendResult[K, V] =>
      originalSender = sender()
      expectedResultSize = c.clusterMembers.size
      var i = 0
      c.clusterMembers foreach { m =>
        val destination = context.actorSelection(m.address + Consts.NODE_ACT_NAME + "/mrra")
        val rMap = c.resArray(i).toMap
        rMap.keys foreach { k =>
          val list = rMap(k).grouped(Consts.CHUNK_SIZE).toList
          expectedResultSize += (list.size-1)
          for (i <- 0 until list.size) {
            destination ! MapResult(k, list(i))
          }
        }
        i += 1
      }
  }

  val handle = expect {
    case ResReceived =>
      results += ResReceived
      if (results.size >= expectedResultSize) processResult()
  }

  def processResult() {
    unexpect(handle)
    originalSender ! Done
    context.stop(self)
  }

}
