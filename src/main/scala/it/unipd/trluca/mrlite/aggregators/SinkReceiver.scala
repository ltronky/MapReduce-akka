package it.unipd.trluca.mrlite.aggregators

import akka.actor.Actor
import akka.cluster.Cluster
import akka.contrib.pattern.Aggregator
import it.unipd.trluca.mrlite.V2Address

import scala.collection.mutable.ArrayBuffer

case class SinkMessage(c:(Int, Seq[(Int, V2Address)]), isComplete:Boolean)
case class SinkCompleted(a:Array[(Int, Seq[(Int, V2Address)])])

class SinkReceiver extends Actor with Aggregator {
  val results = ArrayBuffer.empty[(Int, Seq[(Int, V2Address)])]
  var cSize = Cluster(context.system).state.members.size

  val handle = expect {
    case v:SinkMessage =>
      if (!v.isComplete) {
        cSize += 1
      }
      results += v.c
      if (results.size >= cSize) processResult()
  }

  def processResult() {
    unexpect(handle)
    context.parent ! SinkCompleted(results.toArray)
    context.stop(self)
  }

}
