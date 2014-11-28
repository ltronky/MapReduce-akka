package it.unipd.trluca.arsort

import akka.actor.{ActorRef, Actor}
import akka.cluster.{Cluster, Member}
import akka.contrib.pattern.Aggregator
import it.unipd.trluca.arsort.Messages.MinEMax

import scala.collection.SortedSet
import scala.collection.mutable.ArrayBuffer

case object Get
case object GetMinEMax
case class MM(min:Int, max:Int)

object MinMaxAggregator {
  def minMax(a: Array[Int]) : MM = {
    if (a.isEmpty) return MM(100000, -1)//TODO check range
    a.foldLeft(MM(a(0), a(0)))
    { case (MM(min, max), e) => MM(math.min(min, e), math.max(max, e))}
  }
}

class MinMaxAggregator extends Actor with Aggregator {
  val results = ArrayBuffer.empty[MM]
  var originalSender:ActorRef = null
  var clusterSize:Int = 0

  expectOnce {
    case Get =>
      originalSender = sender()
      val members = Cluster(context.system).state.members
      clusterSize = members.size
      members foreach { m =>
        context.system.actorSelection(m.address + ConstStr.NODE_ACT_NAME) ! MinEMax
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
