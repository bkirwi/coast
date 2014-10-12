package com.monovore.coast
package mini

import akka.actor.{Props, ActorSystem}
import akka.event.Logging
import com.monovore.coast.machine.{Messages, Message, Key}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}

class Cluster(log: Log[String, Key -> Message]) {

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

      case Merge(upstreams) => {
        val upstreamProps = upstreams.map(compile)
        MergeActor.make(upstreamProps, "coast-merges", log)
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
