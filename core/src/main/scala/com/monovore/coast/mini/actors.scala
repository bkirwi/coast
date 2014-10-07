package com.monovore.coast
package mini

import java.util.concurrent.ConcurrentHashMap

import akka.actor.{Actor, ActorRef}

case class Checkpoint(data: Any)
case object Readey
case object Next
case class NewMessage(key: Any, value: Any)

class SpoutActor(spout: String, log: Log[String, Any -> Any]) extends Actor { actor =>

  var offset: Int = 0

  override def receive = bootstrap

  def bootstrap: Receive = {
    case Checkpoint(offset: Int) => {
      actor.offset = offset
    }
    case Readey => {
      context.become(process(context.sender()))
      context.self ! Next
    }
  }

  def process(downstream: ActorRef): Receive = {

    case Next => {

      val next = log.fetch(spout, actor.offset)

      next foreach { case (k -> v) =>

        downstream ! Checkpoint(actor.offset)
        downstream ! NewMessage(k, v)

        actor.offset += 1

        context.self ! Next
      }
    }
  }
}

class TransformActor[S, A, B0, B](upstream: ActorRef, init: S, transformer: A => (S, B0) => (S, Seq[B])) extends Actor { actor =>

  val stateMap: ConcurrentHashMap[A, S] = new ConcurrentHashMap[A, S]()

  override def receive = bootstrap

  def bootstrap: Receive = {
    case Checkpoint(offset: Int) => upstream ! Checkpoint(offset)
    case Readey => context.become(process(context.sender()))
  }

  def process(downstream: ActorRef): Receive = {
    case NewMessage(key: A @unchecked, value: B0 @unchecked) => {
      val state = Option(stateMap.get(key)) getOrElse init
      val (newState, output) = transformer(key)(state, value)
      stateMap.put(key, newState)
      output.foreach { newValue => downstream ! NewMessage(key, newValue) }
    }
  }
}

class SinkActor[A, B](
  upstream: ActorRef,
  checkpoints: String,
  sink: String,
  highWater: Int,
  log: Log[String, Any -> Any]
) extends Actor { actor =>

  var checkpointOffset: Int = 0

  var sinkOffset: Int = 0

  override def receive = bootstrap

  def bootstrap: Receive = {
    case Next => {

      val next = log.fetch(checkpoints, checkpointOffset)

      next match {
        case Some((key: Int) -> value) => {
          upstream ! Checkpoint(value)
          sinkOffset += key
          checkpointOffset += 1
        }
        case None => {
          upstream ! Readey
          context.become(process)
        }
      }
    }
  }

  def process: Receive = {
    case NewMessage(key: A, value: B) => {

      if (sinkOffset >= highWater) {
        log.push(sink, key -> value)
      }

      sinkOffset += 1
    }
  }
}