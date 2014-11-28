package it.unipd.trluca.arsort

import akka.actor.{Props, ActorLogging, Actor}
import akka.cluster.Cluster
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object Messages {
  case class CreateBlock(size: Int)
  case object PrintBlock
  case object MinEMax
}

case class V2Address(address:String, index:Int)
case class SortedArray(a:Array[(Int, V2Address)])

class DistArrayNodeActor extends Actor with ActorLogging with MRJob [Int,Int,Int,(Int, V2Address),Int,Seq[(Int,V2Address)]] {
import it.unipd.trluca.arsort.Messages._

  var originalArray: Array[Int] = Array.empty[Int]
  var sortedArray: Array[(Int,V2Address)] = null

  override def receive = localReceive orElse baseReceive
  def localReceive:Receive = {
    case CreateBlock(size) =>
      originalArray = Array.fill(size)(Random.nextInt(10000))//TODO check range

      if (Cluster(context.system).state.members.head.address == Cluster(context.system).selfAddress)
        context.actorOf(Props[SinkReceiver], "sinkreceiver")

      log.info("Contains {}", originalArray.mkString(","))
      sender() ! Done

    case PrintBlock => println("OriginalArray " + originalArray.mkString(","))

    case MinEMax => sender() ! MinMaxAggregator.minMax(originalArray)

    case SinkCompleted(a) => sortAndRedistribute(a)

    case SortedArray(a)=> sortedArray = a
      log.info("SortedArray " + sortedArray.mkString(","))
  }

  def sortAndRedistribute(a:Array[(Int, Seq[(Int, V2Address)])]): Unit = {
    val ss = a.sortWith(_._1 < _._1)
    val sortedArr = {
      def xx(arr:Array[(Int, Seq[(Int, V2Address)])]):Seq[(Int, V2Address)] = {
        if (arr.size <= 1)
          arr.head._2
        else
          arr.head._2 ++ xx(arr.tail)
      }
      xx(ss).toArray
    }
    val members = Cluster(context.system).state.members.toArray
    val cSize = members.size

    val portion = sortedArr.size / cSize
    var rest = sortedArr.size % cSize
    var start = 0
    members foreach { m=>
      val pp = portion + (if(rest>0) 1 else 0)

      context.actorSelection(m.address + ConstStr.NODE_ACT_NAME) ! SortedArray(sortedArr.slice(start, start+pp))

      start += pp
      rest-=1
    }
  }

////////
  override def partition(k: Int): Int = k

  override def source(): Iterable[(Int, Int)] = new Iterable[(Int,Int)]() {
    override def iterator = new Iterator[(Int, Int)]() {
      val data = originalArray
      var i: Int = 0
      override def hasNext = i < data.size
      def next() = {
        val t = i
        i += 1
        (t, originalArray(t))
      }
    }
  }

  override def mapper(k: Int, v: Int, s: (Int, (Int, V2Address)) => Unit): Unit = {
    val adds = Cluster(context.system).selfAddress.toString
    if (jobC.clusterMembers.size > 1) {
      s(v / (jobC.mmm/ (jobC.clusterMembers.size-1)), (v, V2Address(adds, k)))
    } else {
      s(0, (v,V2Address(adds, k)))
    }
  }

  override def reducer(a: Int, b: Iterable[(Int, V2Address)], sink: ArrayBuffer[(Int, Seq[(Int, V2Address)])]): Unit = {
    if (b != null) {
      val res = b.toArray.sortWith(_._1 < _._1)
      sink += ((res(0)._1, res.toSeq))
    }
  }

  override def sink(s: Iterable[(Int, Seq[(Int, V2Address)])]): Unit = {
    s foreach { item =>
      context.actorSelection(Cluster(context.system).state.members.head.address + ConstStr.NODE_ACT_NAME + "/sinkreceiver") ! SinkMessage(item)
    }
  }
}
