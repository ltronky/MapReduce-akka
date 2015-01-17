package it.unipd.trluca.mrlite.aggregators

import akka.actor.{Actor, ActorRef}
import akka.cluster.Cluster
import akka.contrib.pattern.Aggregator
import it.unipd.trluca.mrlite.Consts
import it.unipd.trluca.mrlite.Messages.MinEMax

import scala.collection.mutable.ArrayBuffer

case object GetMinAndMax
case class MM(min:Int, max:Int)

object MinMaxAggregator {
  def minMax(a: Array[Int]) : MM = {
    if (a.isEmpty) return MM(Int.MaxValue, -1)
    a.foldLeft(MM(a(0), a(0)))
    { case (MM(min, max), e) => MM(math.min(min, e), math.max(max, e))}
  }
}

class MinMaxAggregator extends Actor with Aggregator {
  val results = ArrayBuffer.empty[MM]
  var originalSender:ActorRef = null
  var clusterSize:Int = 0

  expectOnce {
    case GetMinAndMax =>
      originalSender = sender()
      val members = Cluster(context.system).state.members
      clusterSize = members.size
      members foreach { m =>
        context.actorSelection(m.address + Consts.NODE_ACT_NAME) ! MinEMax
      }
  }

  val handle = expect {
    case value:MM =>
      results += value
      if (results.size >= clusterSize) processResult()
  }

  def processResult() {
    unexpect(handle)
    originalSender ! results.reduce((x, y) => MM(if (x.min > y.min) y.min else x.min, if (x.max < y.max) y.max else x.max))
    context.stop(self)
  }
}
