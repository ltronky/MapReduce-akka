package it.unipd.trluca.mrlite

import akka.actor.{Props, ActorLogging, Actor}
import akka.pattern.ask
import it.unipd.trluca.mrlite.aggregators.WorldClock

import scala.concurrent.ExecutionContext.Implicits.global


trait EngineStep
case class StartJob(jC:JobConstants) extends EngineStep
case object Continue extends EngineStep
case object JobTerminated extends EngineStep
case object ExecMap extends EngineStep
case object ExecReduce extends EngineStep


class MRLiteEngine extends Actor with ActorLogging {
  implicit val timeout = Consts.MAIN_TIMEOUT

  var iteration: Int = 0
  var jobC:JobConstants = null


  def receive = {
    case st:StartJob => jobC = st.jC
      log.info("StartJob t=" + System.nanoTime())
      val clockAct = context.actorOf(Props[WorldClock])
      val response = clockAct ? st
      response map { Done =>
        self ! Continue
      }

    case Continue =>
      if (iteration < jobC.iterations) {
        iteration += 1
        self ! ExecMap
      } else
        self ! JobTerminated

    case ExecMap =>
      log.info("PreSourceEMap t=" + System.nanoTime())
      val clockAct = context.actorOf(Props[WorldClock])
      val response = clockAct ? ExecMap
      response map { Done =>
        self ! ExecReduce
      }

    case ExecReduce =>
      log.info("PreReduce t=" + System.nanoTime())
      val clockAct = context.actorOf(Props[WorldClock])
      val response = clockAct ? ExecReduce
      response map { Done =>
        self ! Continue
      }

    case JobTerminated =>
      //log.info("JobTerminated t=" + System.nanoTime())

//    case _=>
    case m:Any => log.info("MessageLost:" + m)// ignore
  }
}
