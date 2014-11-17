package it.unipd.trluca.arsort

import akka.actor.{ActorRef, Actor}
import akka.cluster.Member
import akka.contrib.pattern.Aggregator
import it.unipd.trluca.arsort.Messages.MinEMax

import scala.collection.SortedSet
import scala.collection.mutable.ArrayBuffer

case object GetMinEMax

object MinMaxAggregator {
  def minMax(a: Array[Int]) : (Int, Int) = {
    if (a.isEmpty) return (100000, -1)
    a.foldLeft((a(0), a(0)))
    { case ((min, max), e) => (math.min(min, e), math.max(max, e))}
  }
}

class MinMaxAggregator extends Actor with Aggregator {
  val results = ArrayBuffer.empty[(Int, Int)]
  var originalSender:ActorRef = null
  var clusterSize:Int = 0

  expectOnce {
    case x:SortedSet[Member] =>
      originalSender = sender()
      clusterSize = x.size
      x foreach { m =>
        context.system.actorSelection(m.address + ConstStr.NODE_ACT_NAME) ! MinEMax
      }
  }

  val handle = expect {
    case value:(Int, Int) =>
      results += value
      if (results.size >= clusterSize) processResult()
  }


  def processResult() {
    unexpect(handle)

    originalSender ! results.reduce((x, y) => (if (x._1 > y._1) y._1 else x._1, if (x._2 < y._2) y._2 else x._2))
    context.stop(self)
  }
}
