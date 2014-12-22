package it.unipd.trluca.mrlite

import akka.actor.{Address, Props, ActorLogging, Actor}
import akka.cluster.Cluster
import it.unipd.trluca.mrlite.aggregators._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object Messages {
  case class CreateBlock(size: Int, valueRange:Int, mainNodeAddress:Address)
  case object PrintBlock
  case object MinEMax
}

case class V2Address(address:String, index:Int)
case class SortedArray(a:Array[(Int, V2Address)], isComplete:Boolean)

class DistArrayNodeActor extends Actor with ActorLogging with MRJob [Int,Int,Int,(Int, V2Address),Int,Seq[(Int,V2Address)]] {
import it.unipd.trluca.mrlite.Messages._

  var originalArray: Array[Int] = Array.empty[Int]
  var sortedArray: Array[(Int,V2Address)] = Array.empty

  var mainNodeAddress:Address = null

  override def receive = super.receive orElse localReceive
  def localReceive:Receive = {
    case CreateBlock(size, valueRange, mAddress) =>
      mainNodeAddress = mAddress
      originalArray = Array.fill(size)(Random.nextInt(valueRange))

      if (mainNodeAddress == Cluster(context.system).selfAddress)
        context.actorOf(Props[SinkReceiver], "sinkreceiver")

//      log.info("Contains {}", originalArray.mkString(","))
      log.info("Array Block Created")
      sender() ! Done

    case PrintBlock => println("OriginalArray created") //+ originalArray.mkString(","))

    case MinEMax => sender() ! MinMaxAggregator.minMax(originalArray)

    case SinkCompleted(a) => sortAndRedistribute(a)

    case SortedArray(a, isComplete) =>
      sortedArray = sortedArray ++ a
      if (isComplete) {
        //log.info("SortedArray complete " + sortedArray.mkString(","))
        context.system.shutdown() //TODO check shutdown
      }
//    case _=> //ignore
    case m:Any => log.info("MessageLost:" + m)// ignore
  }

  def sortAndRedistribute(a:Array[(Int, Seq[(Int, V2Address)])]): Unit = {
    log.info("MidSink t=" + System.nanoTime())
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
      val block = sortedArr.slice(start, start+pp)
      val destination = context.actorSelection(m.address + Consts.NODE_ACT_NAME)

      val list = block.grouped(Consts.CHUNK_SIZE).toList
      for (i <- 0 until list.size) {
        if (i != list.size-1) {
          destination ! SortedArray(list(i), isComplete = false)
        } else {
          destination ! SortedArray(list(i), isComplete = true)
        }
      }

      start += pp
      rest-=1
    }
    log.info("JobTerminated t=" + System.nanoTime())
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
    val destination = context.actorSelection(mainNodeAddress + Consts.NODE_ACT_NAME + "/sinkreceiver")

    s foreach { item =>
      val list = item._2.grouped(Consts.CHUNK_SIZE).toList
      for (i <- 0 until list.size) {
        if (i != list.size-1) {
          destination ! SinkMessage((item._1, list(i)), isComplete = false)
        } else {
          destination ! SinkMessage((item._1, list(i)), isComplete = true)
        }
      }

    }
  }
}
