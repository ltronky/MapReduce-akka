package it.unipd.trluca.arsort

import akka.actor.{ActorRef, Actor}
import akka.cluster.Cluster
import akka.contrib.pattern.Aggregator

import scala.collection.mutable.ArrayBuffer

case class SinkMessage(c:(Int, Seq[(Int, V2Address)]))

class SinkReceiver extends Actor with Aggregator {
  val results = ArrayBuffer.empty[(Int, Seq[(Int, V2Address)])]
  val cSize = Cluster(context.system).state.members.size

  val handle = expect {
    case v:SinkMessage =>
      results += v.c
      if (results.size >= cSize) processResult()
  }

  def processResult() {
    unexpect(handle)
    context.parent ! results.toArray
    context.stop(self)
  }

}
