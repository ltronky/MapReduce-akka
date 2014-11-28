package it.unipd.trluca.mrlite

import akka.actor.{Props, ActorLogging, Actor}
import akka.pattern.ask
import it.unipd.trluca.mrlite.aggregators.WorldClock

import scala.concurrent.ExecutionContext.Implicits.global


trait EngineStep
case class StartJob(jC:JobConstants) extends EngineStep
case object Continue extends EngineStep
case object JobTerminated extends EngineStep
case object ExecSource extends EngineStep
case object ExecMap extends EngineStep
case object ExecReduce extends EngineStep
case object Sink extends EngineStep


class MRLiteEngine extends Actor with ActorLogging {
  implicit val timeout = ConstStr.MAIN_TIMEOUT

  var iteration: Int = 0
  var jobC:JobConstants = null


  def receive = {
    case st:StartJob => jobC = st.jC
      log.info("StartJob")
      val clockAct = context.actorOf(Props[WorldClock])
      val response = clockAct ? st
      response map { Done =>
        self ! Continue
      }

    case Continue =>
      if (iteration < jobC.iterations) {
        iteration += 1
        self ! ExecSource
      } else
        self ! JobTerminated

    case ExecSource =>
      log.info("Source")
      val clockAct = context.actorOf(Props[WorldClock])
      val response = clockAct ? ExecSource
      response map { Done =>
        self ! ExecMap
      }

    case ExecMap =>
      log.info("Map")
      val clockAct = context.actorOf(Props[WorldClock])
      val response = clockAct ? ExecMap
      response map { Done =>
        self ! ExecReduce
      }

    case ExecReduce =>
      log.info("Reduce")
      val clockAct = context.actorOf(Props[WorldClock])
      val response = clockAct ? ExecReduce
      response map { Done =>
        self ! Sink
      }

    case Sink =>
      log.info("Sink")
      val clockAct = context.actorOf(Props[WorldClock])
      val response = clockAct ? Sink
      response map { Done =>
        self ! Continue
      }

    case JobTerminated =>
      log.info("JobTerminated")

    case _=>
  }
}
