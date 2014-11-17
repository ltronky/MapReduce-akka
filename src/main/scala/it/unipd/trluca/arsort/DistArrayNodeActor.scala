package it.unipd.trluca.arsort

import akka.actor.{ActorLogging, Actor}
import akka.cluster.Cluster
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object Messages {
  case class CreateBlock(size: Int)
  case object PrintBlock
  case object MinEMax
}

case class V2Address(address:String, index:Int)

class DistArrayNodeActor extends Actor with ActorLogging with MRJob [Int,Int,Int,(Int, V2Address),Int,Seq[(Int,V2Address)]] {
import it.unipd.trluca.arsort.Messages._

  var originalArray: Array[Int] = Array.empty[Int]
  var sortedArray: Array[Int] = Array.empty[Int]
  
  def receive = localReceive orElse baseReceive
  def localReceive:Receive = {
    case CreateBlock(size) =>
      originalArray = Array.fill(size)(Random.nextInt(10000))
      log.info("Contains {}", originalArray.mkString(","))

    case PrintBlock => println("Here is contained " + originalArray.mkString(","))

    case MinEMax => sender() ! MinMaxAggregator.minMax(originalArray)
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
    if (jobC.clusterSize > 1) {
      val span = jobC.max - jobC.min
      s(v / (span/ (jobC.clusterSize-1)), (v, V2Address(adds, k)))
    } else {
      s(0, (v,V2Address(adds, k)))
    }
  }

  override def reducer(a: Int, b: Iterable[(Int, V2Address)], sink: ArrayBuffer[(Int, Seq[(Int, V2Address)])]): Unit = {
    if (b != null) {
      var size:Int = 0
      b foreach {_=> size+=1}

      val r:Array[(Int,V2Address)] = new Array[(Int,V2Address)](size)
      var i:Int = 0
      b foreach {x=> r(i)=x; i+=1}

      val res = r.sortWith(_._1 < _._1)

      val t = (res(0)._1, res.toSeq)
      sink += t
    }
  }

  override def sink(s: Iterable[(Int, Seq[(Int, V2Address)])]): Unit = {
    s foreach { item => //TODO???
      log.info("key=> " + item._1.toString + " values=> " + item._2.mkString(","))
    }
  }
}
