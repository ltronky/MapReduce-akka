package it.unipd.trluca.arsort

import akka.actor.{ActorRef, Actor}
import akka.cluster.Cluster
import akka.contrib.pattern.Aggregator
import it.unipd.trluca.arsort.Messages.CreateBlock

import scala.collection.mutable.ArrayBuffer

case class InitArray(arraySize:Int)

class InitAggregator extends Actor with Aggregator {
  val results = ArrayBuffer.empty[Unit]
  var originalSender:ActorRef = null
  var clusterSize:Int = 0

  expectOnce {
    case InitArray(distArraySize) =>
      val members = Cluster(context.system).state.members
      clusterSize = members.size
      originalSender = sender()
      val portion = distArraySize / clusterSize
      var rest = distArraySize % clusterSize
      members foreach { member =>
        context.system.actorSelection(member.address + ConstStr.NODE_ACT_NAME) !
          CreateBlock(portion + (if (rest > 0) 1 else 0))
        rest -= 1
      }
  }

  val handle = expect {
    case Done =>
      results += Done
      if (results.size >= clusterSize) processResult()
  }

  def processResult() {
    unexpect(handle)
    originalSender ! Done
    context.stop(self)
  }
}
