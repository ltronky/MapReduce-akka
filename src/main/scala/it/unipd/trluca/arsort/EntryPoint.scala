package it.unipd.trluca.arsort

import akka.actor._
import akka.cluster._
import akka.pattern.ask
import akka.util.Timeout

import it.unipd.trluca.arsort.Messages._
import spray.routing._
import spray.routing.Directives._

import scala.collection.SortedSet
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import scala.concurrent.ExecutionContext.Implicits.global

object ConstStr {
  final val NODE_ACT_NAME = "/user/ablock"
  final val MAIN_TIMEOUT = Timeout(10.seconds) //TODO controllare non sia troppo breve per l'esecuzione
}

class EntryPoint extends HttpService with Actor with ActorLogging {
  def actorRefFactory = context
  implicit val timeout = ConstStr.MAIN_TIMEOUT
  val cluster = Cluster(context.system)

  var mm = (0, 0)

  def receive = runRoute(
    path("init") {
      parameter("dim") { dimstring =>
        post {
          val dim = dimstring.toInt
          val members = cluster.state.members
          val portion = dim / members.size
          var rest = dim % members.size
          members foreach { member =>
            context.system.actorSelection(member.address + ConstStr.NODE_ACT_NAME) ! CreateBlock(portion + (if(rest>0) 1 else 0))
            rest-=1
          }
          complete("Inizializzazione avviata\n")
        }
      }
    } ~ path("printiarray") {
      get {
        complete ({
          cluster.state.members foreach { member =>
            context.system.actorSelection(member.address + ConstStr.NODE_ACT_NAME) ! PrintBlock
          }
          "PrintInit members=>" + cluster.state.members.size.toString + "\n"
        })
      }
    } ~ path("minmax") {
      get {
        complete ({
          val aggr = context.system.actorOf(Props[MinMaxAggregator])
          val response = (aggr ? cluster.state.members).mapTo[(Int, Int)]
          response map { res:(Int, Int) =>
            mm = res
            log.info("Min " + mm._1.toString + " & Max " + mm._2.toString)
          }
          "Min & Max\n"
        })
      }
    } ~ path("start") {
      get {
        complete ({
          val engine = context.actorOf(Props[MRLiteEngine], "Engine")
          engine ! StartJob(JobConstants(1, mm._1, mm._2, cluster.state.members.size))
          "Engine Started\n"
        })
      }
    }
  )

}
