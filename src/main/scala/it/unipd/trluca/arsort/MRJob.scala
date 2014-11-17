package it.unipd.trluca.arsort

import akka.actor.{Props, Actor}
import akka.cluster.Cluster
import akka.pattern.ask
import akka.util.Timeout


import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext.Implicits.global

case class JobConstants(iterations:Int, min:Int, max:Int, clusterSize:Int)

object MRJob {
  def insert[K2,V2](a:mutable.HashMap[K2,ArrayBuffer[V2]], k:K2, v:V2) {
    var gr = if (a.contains(k)) a(k) else new ArrayBuffer[V2]()
    gr += v
    a += ((k, gr))
  }

  def insert[K2,V2](a:mutable.HashMap[K2,ArrayBuffer[V2]], k:K2, v:ArrayBuffer[V2]) {
    var gr = if (a.contains(k)) a(k) else new ArrayBuffer[V2]()
    gr ++= v
    a += ((k, gr))
  }
}

trait MRJob[K1,V1,K2,V2,K3,V3] {
  self: Actor =>
  def baseReceive:Receive = {
    case StartJob(j) =>
      jobC = j
      init()
      sender() ! Done

    case ExecSource =>
      src = source()
      sender() ! Done

    case ExecMap =>
      val orSender = sender()
      if (src != null)
        src foreach {x => mapper(x._1, x._2, mSink)}

      //Transmit data to all nodes
      val resDisp = context.actorOf(Props[ResultDispatcher[K2,V2]])
      val response = resDisp ? SendResult(Cluster(context.system).state.members, results)
      response map { Done =>
        orSender ! Done
      }

    case ExecReduce =>
      val orSender = sender()
      val response = (mapResRecActor ? MapResultGet).mapTo[Map[K2,ArrayBuffer[V2]]]
      response map { res:Map[K2,ArrayBuffer[V2]] =>
        res.keySet foreach { k =>
          reducer(k, res(k), output)
        }
        orSender ! Done
      }

    case Sink =>
      sink(output)
      sender() ! Done

    case _=>
  }

  implicit val timeout = ConstStr.MAIN_TIMEOUT

  val mapResRecActor = context.actorOf(Props[MapResultReceiverActor[K2,V2]], "mrra")

  var jobC:JobConstants = null

  var src: Iterable[(K1, V1)] = null
  var results:Array[mutable.HashMap[K2,ArrayBuffer[V2]]] = null
  val mSink = (k:K2,v:V2) => {MRJob.insert[K2, V2](results(partition(k) % jobC.clusterSize), k, v)}
  val output:ArrayBuffer[(K3,V3)] = ArrayBuffer.empty

  def init():Unit = {
    results = Array.fill(jobC.clusterSize)(new mutable.HashMap[K2, ArrayBuffer[V2]])
  }


  def partition(k:K2):Int

  def source():Iterable[(K1,V1)]

  def sink(s:Iterable[(K3,V3)]):Unit

  def mapper(k:K1, v:V1, s:(K2,V2)=>Unit):Unit

  def reducer(a:K2, b:Iterable[V2], sink:ArrayBuffer[(K3,V3)]):Unit
}