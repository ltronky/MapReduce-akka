package it.unipd.trluca.arsort

import akka.actor.{ActorRef, Actor}
import akka.cluster.{Cluster, Member}
import akka.contrib.pattern.Aggregator

import scala.collection.SortedSet
import scala.collection.mutable.ArrayBuffer

case object Done

class WorldClock extends Actor with Aggregator {
  val results = ArrayBuffer.empty[Unit]
  var originalSender:ActorRef = null
  val members = Cluster(context.system).state.members

  expectOnce {
    case es:EngineStep =>
      originalSender = sender()
      members foreach { m =>
        context.system.actorSelection(m.address + ConstStr.NODE_ACT_NAME) ! es
      }
  }

  val handle = expect {
    case Done =>
      results += Done
      if (results.size >= members.size) processResult()
  }

  def processResult() {
    unexpect(handle)
    originalSender ! Done
    context.stop(self)
  }

}
