package it.unipd.trluca.arsort

import akka.actor.{ActorRef, Actor}
import akka.cluster.Member
import akka.contrib.pattern.Aggregator

import scala.collection.SortedSet
import scala.collection.mutable.ArrayBuffer

case object Done
case class Command(members:SortedSet[Member], es:EngineStep)

class WorldClock extends Actor with Aggregator {
  val results = ArrayBuffer.empty[Unit]
  var originalSender:ActorRef = null
  var clusterSize:Int = 0

  expectOnce {
    case c:Command =>
      originalSender = sender()
      clusterSize = c.members.size
      c.members foreach { m =>
        context.system.actorSelection(m.address + ConstStr.NODE_ACT_NAME) ! c.es
      }
  }

  val handle = expect {
    case v =>
      results += v
      if (results.size >= clusterSize) processResult()
  }

  def processResult() {
    unexpect(handle)
    originalSender ! Done
    context.stop(self)
  }

}
