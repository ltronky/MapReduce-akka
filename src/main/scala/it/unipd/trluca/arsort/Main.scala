package it.unipd.trluca.arsort

import akka.actor.{ActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.ClusterDomainEvent
import com.typesafe.config.ConfigFactory
import akka.io.IO
import spray.can.Http


object Main {
  def main(args: Array[String]): Unit = {
    val systemName = "ClusterSystem"


    args foreach { port =>
      val conf = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port")
        .withFallback(ConfigFactory.load("app_debug"))
      val sys = ActorSystem(systemName, conf)

      sys.actorOf(Props[MemberListener], "memberListener")
      sys.actorOf(Props[DistArrayNodeActor], "ablock")

      if (port == "2551") {
        val ep = sys.actorOf(Props[EntryPoint], "ep")
        implicit val system = sys
        IO(Http) ! Http.Bind(ep, interface = "127.0.0.1", port = 5000)
      }
    }

//    val conf = ConfigFactory.parseString(s"akka.remote.netty.tcp.hostname=" + args(0))
//      .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.port=" + args(1)))
//      .withFallback(ConfigFactory.load())
//    val sys = ActorSystem(systemName, conf)
//
//    sys.actorOf(Props[MemberListener], "memberListener")
//    sys.actorOf(Props[DistArrayNodeActor], "ablock")
//
//    if (args(0) == "192.168.56.11") {
//      val ep = sys.actorOf(Props[EntryPoint], "ep")
//      implicit val system = sys
//      IO(Http) ! Http.Bind(ep, interface = "192.168.56.11", port = 5000)
//    }
  }
}
