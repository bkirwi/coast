package com.monovore.coast
package mini

import java.util.concurrent.ConcurrentHashMap

import akka.actor._
import akka.event.Logging
import com.monovore.coast.machine.{Messages, Message, Key}
import com.typesafe.config.{ConfigValue, ConfigValueFactory, ConfigFactory}

import collection.JavaConverters._

case class Checkpoint(current: Int, upstream: Int, stateChange: Option[Any -> Any])
case class Offset(offset: Int, stateChange: Option[Any -> Any])
case class Upstream(data: Any)
case object Ready
case object Next
case class NewMessage(key: Any, value: Any)

class SourceActor(spout: String, log: Log[String, Key -> Message]) extends Actor { actor =>

  var offset: Int = 0

  override def receive = bootstrap

  def bootstrap: Receive = {
    case Offset(offset, None) => {
      actor.offset = offset
    }
    case Ready => {
      println(s"Starting source '$spout' with offset $offset")
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

  val upstream = context.actorOf(upstreamProps, "transformed")

  val stateMap: ConcurrentHashMap[A, S] = new ConcurrentHashMap[A, S]()

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
      println("Start state: " + stateMap.asScala)
      context.become(process(context.sender()))
      upstream ! Ready
    }
  }

  def process(downstream: ActorRef): Receive = {
    case NewMessage(key: A @unchecked, value: B0 @unchecked) => {

      println(key, value, actor.offset, actor.highWater)

      if (actor.offset >= actor.highWater) {

        val state = Option(stateMap.get(key)) getOrElse init

        println(state)

        val (newState, output) = transformer(key)(state, value)

        stateMap.put(key, newState)

        output.foreach { newValue =>

          println("KV", key, newValue)

          downstream ! NewMessage(key, newValue)
        }

        downstream ! Offset(actor.offset + 1, Some(key -> newState))
      }

      actor.offset += 1
    }
    case Offset(offset, change) => downstream ! Checkpoint(actor.offset, offset, change)
//    case other => downstream ! Upstream(other)
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
        println("SINK", key, value, offset, highWater)
        val returnedOffset = log.push(sink, Key(key) -> Message(value))

        if (returnedOffset != offset) {
          println("SOMETHING'S WRONG", key, value, offset, highWater)
          sys.error("SOMETHING'S WRONG")
        }
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

class Actors(log: Log[String, Key -> Message]) {

  def this() { this(Log.empty()) }

  def send[A, B](name: Name[A, B], messages: (A -> B)*): Unit = {
    messages
      .map { case (k, v) => Key(k) -> Message(v) }
      .foreach { pair => log.push(name.name, pair) }
  }

  def messages(names: Name[_, _]*): Messages = Messages(
    names
      .map { name =>
        name.name -> log.fetchAll(name.name).groupByKey
      }
      .toMap
  )

  def withRunning(flow: Flow[_])(block: ActorSystem => Unit): Unit = {
    val running = run(flow)
    block(running)
    running.shutdown()
    running.awaitTermination()
  }

  def run(flow: Flow[_]): ActorSystem = {
    
    def compile[A, B](element: Element[A, B]): Props = element match {

      case Source(name) => SourceActor.make(name, log)

      case Transform(upstream, init, transformer) => {
        val upstreamProps = compile(upstream)
        TransformActor.make(upstreamProps, init, transformer)
      }

    }

    val roots = flow.bindings
      .map { case (name, element) =>

        val upstream = compile(element)

        val sink = SinkActor.make(upstream, name, log)

        sink
      }

    val config = ConfigFactory.empty()
      .withValue("akka.log-dead-letters-during-shutdown", ConfigValueFactory.fromAnyRef(false))
      .withValue("akka.actor.debug.unhandled", ConfigValueFactory.fromAnyRef(true))

    val actors = ActorSystem.create("whatever", config)

    actors.eventStream.setLogLevel(Logging.DebugLevel)

    roots.map { p =>
      val ref = actors.actorOf(p)
      ref ! Next
      ref
    }

    actors
  }
}