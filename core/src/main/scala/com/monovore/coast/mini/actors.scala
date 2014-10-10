package com.monovore.coast
package mini

import akka.actor._
import akka.event.Logging
import com.monovore.coast.machine.{Key, Message}

import scala.collection.mutable

case class Checkpoint(current: Int, upstream: Int, stateChange: Option[Any -> Any])
case class Offset(offset: Int, stateChange: Option[Any -> Any])
case class Upstream(data: Any)
case object Ready
case object Next
case class NewMessage(key: Any, value: Any)

class SourceActor(spout: String, log: Log[String, Key -> Message]) extends Actor { actor =>

  val logger = Logging(context.system, this)

  var offset: Int = 0

  override def receive = bootstrap

  def bootstrap: Receive = {
    case Offset(offset, None) => {
      actor.offset = offset
    }
    case Ready => {
      logger.debug(s"Starting source '$spout' with offset $offset")
      context.become(process(context.sender()))
      context.self ! Next
    }
  }

  def process(downstream: ActorRef): Receive = {

    case Next => {

      val next = log.fetch(spout, actor.offset)

      next foreach { case (k -> v) =>

        downstream ! NewMessage(k.get, v.get)

        actor.offset += 1
        downstream ! Offset(actor.offset, None)

        context.self ! Next
      }
    }
  }
}

object SourceActor {
  
  def make(name: String, log: Log[String, Key -> Message]): Props
    = Props(new SourceActor(name, log))
  
}

class TransformActor[S, A, B0, B](upstreamProps: Props, init: S, transformer: A => (S, B0) => (S, Seq[B])) extends Actor { actor =>

  val logger = Logging(context.system, this)

  val upstream = context.actorOf(upstreamProps, "transformed")

  val stateMap: mutable.Map[A, S] = mutable.Map[A, S]()

  var offset: Int = 0

  var highWater: Int = 0

  override def receive = bootstrap

  def bootstrap: Receive = {
    case Offset(offset, change) => {

      actor.highWater = offset

      change.foreach { case (k: A @unchecked, s: S @unchecked) =>
        stateMap.put(k, s)
      }
    }
    case Checkpoint(current, other, change) => {

      actor.offset = current

      upstream ! Offset(other, change)
    }
    case Ready => {

      logger.debug(s"Starting transform with offset of $offset and high-water of $highWater")

      context.become(process(context.sender()))
      upstream ! Ready
    }
    case Upstream(other) => upstream ! other
  }

  def process(downstream: ActorRef): Receive = {
    case NewMessage(key: A @unchecked, value: B0 @unchecked) => {

      if (actor.offset >= actor.highWater) {

        val state = stateMap.getOrElse(key, init)

        val (newState, output) = transformer(key)(state, value)

        stateMap.put(key, newState)

        output.foreach { newValue =>
          downstream ! NewMessage(key, newValue)
        }

        downstream ! Offset(actor.offset + 1, Some(key -> newState))
      }

      actor.offset += 1
    }
    case Offset(offset, change) => downstream ! Checkpoint(actor.offset, offset, change)
    case other => downstream ! Upstream(other)
  }
}

object TransformActor {
  def make[S, A, B0, B](upstream: Props, init: S, transformer: A => (S, B0) => (S, Seq[B])): Props
    = Props(new TransformActor(upstream, init, transformer))
}

class SinkActor(
  upstreamProps: Props,
  sink: String,
  log: Log[String, Key -> Message]
) extends Actor { actor =>

  val upstream = context.actorOf(upstreamProps, sink)

  var checkpointOffset: Int = 0

  var offset: Int = 0

  var highWater: Int = log.fetchAll(sink).size // TODO: ???

  val checkpoints: String = s"coast-cp-$sink"

  override def receive = bootstrap

  def bootstrap: Receive = {
    case Next => {

      val next = log.fetch(checkpoints, checkpointOffset)

      next match {
        case Some(_ -> Message(Checkpoint(current, up, change))) => {
          upstream ! Offset(up, change)

          offset = current

          checkpointOffset += 1
          context.self ! Next
        }
        case Some(_ -> Message(Upstream(value))) => {
          upstream ! value
          checkpointOffset += 1
          context.self ! Next
        }
        case None => {
          upstream ! Ready
          context.become(process)
        }
        case _ => ()
      }
    }
  }

  def process: Receive = {
    case NewMessage(key, value) => {

      if (offset >= highWater) {

        val returnedOffset = log.push(sink, Key(key) -> Message(value))

        require (returnedOffset == offset, "offset must match expected, since there's only one writer")
      }

      offset += 1
    }
    case Offset(value, change) => {
      log.push(checkpoints, Key(unit) -> Message(Checkpoint(offset, value, change)))
      checkpointOffset += 1
    }
    case other => {
      log.push(checkpoints, Key(unit) -> Message(Upstream(other)))
      checkpointOffset += 1
    }
  }
}

object SinkActor {
  def make(upstream: Props, sink: String, log: Log[String, Key -> Message]): Props
    = Props(new SinkActor(upstream, sink, log))
}

class MergeActor(upstreamProps: Seq[Props], logName: String, log: Log[String, Key -> Message]) extends Actor { actor =>

  val upstreams =
    upstreamProps.zipWithIndex
      .map { case (ps, ix) =>
        val ref = context.actorOf(ps, s"merged-$ix")
        ref -> mutable.Queue[NewMessage]()
      }

  var offset: Int = 0

  var highWater: Int = 0

  override def receive: Receive = {
    case Offset(offset, None) => actor.offset = offset
    case Ready => {
      self ! Ready
      context.become(process(context.sender()))
    }
  }

  def process(downstream: ActorRef): Receive = {
    case Ready => {

      val nextChoice = log.fetch(logName, offset)

      nextChoice match {
        case Some(_ -> Message(ix: Int)) => { // pre-chosen

          val (ref, queue) = upstreams(ix)

          if (queue.nonEmpty) {
            val toSend = queue.dequeue()
            downstream ! toSend
            actor.offset += 1
            downstream ! Offset(actor.offset, None)
            actor.self ! Ready
          }
        }
        case None => {
          upstreams.foreach { case (_, queue) =>
            queue.foreach { context.self ! _ }
            queue.clear()
          }
        }
      }
    }
  }
}
