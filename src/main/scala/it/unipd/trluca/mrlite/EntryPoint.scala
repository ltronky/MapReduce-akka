package it.unipd.trluca.mrlite

import akka.actor._
import akka.cluster._
import akka.pattern.ask
import akka.util.Timeout
import it.unipd.trluca.mrlite.aggregators._

import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext.Implicits.global

object ConstStr {
  final val NODE_ACT_NAME = "/user/ablock"
  final val MAIN_TIMEOUT = Timeout(10.seconds) //TODO controllare non sia troppo breve per l'esecuzione
}

case object StartExecution
case object MinMax
case object StartEngine
case class SetDistArraySize(c:Config)

class EntryPoint extends Actor with ActorLogging {
  def actorRefFactory = context
  implicit val timeout = ConstStr.MAIN_TIMEOUT

  var distArraySize = 0
  var valueRange = 0
  var mmm:Int = 0

  def receive = {
    case SetDistArraySize(config) =>
      distArraySize = config.arraySize
      valueRange = config.valueRange
      val mL = context.actorOf(Props[MemberListener], "memberListener")
      mL ! SetInitClusterSize(config.clusterSize)

    case StartExecution =>
      val initAggr = context.actorOf(Props[InitAggregator])
      val response = initAggr ? InitArray(distArraySize, valueRange)
      response map { Done =>
        self ! MinMax
      }

    case MinMax =>
      val aggr = context.actorOf(Props[MinMaxAggregator])
      val response = (aggr ? GetMinEMax).mapTo[MM]
      response map { res:MM =>
        mmm = res.max-res.min
        log.info("Min " + res.min.toString + " & Max " + res.max.toString)

        self ! StartEngine
      }

    case StartEngine =>
      val engine = context.actorOf(Props[MRLiteEngine], "engine")
      engine ! StartJob(JobConstants(1, mmm, Cluster(context.system).state.members))
  }
}
