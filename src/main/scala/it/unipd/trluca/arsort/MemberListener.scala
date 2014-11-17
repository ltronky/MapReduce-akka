package it.unipd.trluca.arsort

import akka.actor.{Actor, ActorLogging, Address}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._

class MemberListener extends Actor with ActorLogging {

  val cluster = Cluster(context.system)

  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberEvent])
    cluster.subscribe(self, classOf[LeaderChanged])
  }

  override def postStop(): Unit =
    cluster unsubscribe self

  var nodes = Set.empty[Address]

  def receive = {
    case state: CurrentClusterState =>
      log.info("Current members: {}", state.members.mkString(", "))
    case MemberUp(member) =>
      nodes += member.address
      log.info("Member is Up: {}. {} nodes in cluster",
        member.address, nodes.size)
    case MemberRemoved(member, _) =>
      nodes -= member.address
      log.info("Member is Removed: {}. {} nodes cluster",
        member.address, nodes.size)
    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)

    case LeaderChanged(address) =>
      cluster unsubscribe self
      if (Cluster(context.system).selfAddress == address.get) {
        cluster.subscribe(self, classOf[MemberEvent])
        cluster.subscribe(self, classOf[LeaderChanged])
        log.info(s"leader changed: $address")
      } else {
        cluster.subscribe(self, classOf[LeaderChanged])
      }

    case _: MemberEvent => // ignore
    case ss:String => log.info(ss)
  }

}