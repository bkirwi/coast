package com.monovore.coast
package mini

import java.util.concurrent.{Executors, LinkedBlockingQueue, BlockingQueue}

import com.monovore.coast.machine.{Messages, Key, Message}

import scala.util.control.NonFatal

trait Thing[+A, +B]
case class Data[+A, +B](key: A, value: B) extends Thing[A, B]
case class Offset(offset: Int) extends Thing[Nothing, Nothing]
case object Ready extends Thing[Nothing, Nothing]

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

  abstract class Task { task =>

    @volatile private[this] var _ready: Boolean = false
    def ready = _ready

    @volatile private[this] var _idle: Boolean = false
    def idle = _idle

    def prod(): Unit = {
      if (!ready) _ready = {
        val x = prepare()
        if (x) println("READY!!")
        x
      }
      else _idle = step()
    }

    def prepare(): Boolean

    def step(): Boolean
  }

  def whileRunning[A](flow: Flow[_])(block: RunningFlow => A): A = {

    val r = compile(flow)
    val a = block(r)
    r.stop()
    a
  }

  def compile(flow: Flow[_]): RunningFlow = {

    def compile[A, B](
      element: Element[A, B],
      downstream: BlockingQueue[Thing[A, B]]
    ): Seq[Task] = element match {

      case Source(name) => {

        val task = new Task {

          private[this] var offset: Int = 0

          override def prepare(): Boolean = {

            val next = Option(downstream.poll())

            next collect {
              case Offset(o) => offset = o
            }

            next.collect { case Ready => true } getOrElse false
          }

          def step(): Boolean = {

            val fetched = log.fetch(name, offset)

            fetched foreach { case k -> v =>
              downstream.put(Offset(offset))
              downstream.put(Data(k.cast, v.cast))
              offset += 1
            }

            fetched.isEmpty
          }
        }

        Seq(task)
      }

      case Transform(upstream, init, transformer) => {

        def doFor[S, B0](queue: BlockingQueue[Thing[A, B]], upstream: Element[A, B0], init: S, transformer: A => (S, B0) => (S, Seq[B])): Seq[Task] = {

          val upQueue = new LinkedBlockingQueue[Thing[A, B0]]()

          val upstreamTask = compile(upstream, upQueue)

          val myTask = new Task {

            @volatile private[this] var state: Map[A, S] = Map.empty.withDefaultValue(init)

            override def prepare(): Boolean = {

              val next = Option(downstream.poll())

              next collect {
                case Offset(o) => upQueue.put(Offset(o))
                case Ready => upQueue.put(Ready)
              }

              next.collect { case Ready => true } getOrElse false
            }

            override def step(): Boolean = {

              val next = Option(upQueue.poll())

              next foreach {
                case Data(k, msg) => {
                  val (newState, out) = transformer(k)(state(k), msg)
                  state += (k -> newState)
                  out.foreach { msg => queue.put(Data(k, msg)) }
                }
                case Offset(o) => {
                  queue.put(Offset(o))
                }
              }

              next.isEmpty
            }
          }

          upstreamTask :+ myTask
        }

        doFor(downstream, upstream, init, transformer)
      }
    }

    def compileLast[A, B](sink: String, element: Element[A, B]): Seq[Task] = {

      val queue = new LinkedBlockingQueue[Thing[A, B]]()

      val checkpoints = s"$sink-checkpoints"

      val (upstreamCheckpoint, myCheckpoint) =
        log.fetchAll(checkpoints).lastOption
          .map { case (k, v) => v.cast[Int -> Int] }
          .getOrElse(0 -> 0)

      println(s"Starting from checkpoint: $upstreamCheckpoint -> $myCheckpoint")

      val upstream = compile(element, queue)

      val downstream = new Task {

        private[this] var checkpointOffset: Int = 0

        private[this] var offset: Int = 0

        private[this] val skipUntil: Int = log.fetchAll(sink).size

        override def prepare(): Boolean = {

          val next = log.fetch(checkpoints, checkpointOffset)

          next foreach { msg =>
            val (upstream, checkpoint) = msg._2.cast[Int -> Int]
            offset = checkpoint
            checkpointOffset += 1
            queue.put(Offset(upstream))
          }

          if (next.isEmpty) queue.put(Ready)

          next.isEmpty
        }

        def step(): Boolean = {

          val next = Option(queue.poll())

          next foreach {
            case Data(k, msg) => {
              if (offset >= skipUntil) log.push(sink, Key(k) -> Message(msg))
              offset += 1
            }
            case Offset(o) => {
              log.push(checkpoints, Key(unit) -> Message(o -> offset))
            }
          }

          next.isEmpty
        }
      }

      upstream :+ downstream
    }

    val tasks = flow.state.map { case (name, element) =>
      compileLast(name, element)
    }

    new RunningFlow(tasks.flatten.toSeq)
  }

  class RunningFlow(tasks: Seq[Task]) {

    val executor = Executors.newFixedThreadPool(4)

    tasks foreach { task =>

      val runnable = new Runnable {
        override def run(): Unit = {

          try {
            task.prod()
          } catch {
            case NonFatal(e) => e.printStackTrace()
          }

          executor.submit(this)
        }
      }

      executor.submit(runnable)
    }


    def stop(): Unit = executor.shutdownNow()

    def complete(): Unit = tasks foreach { task =>

      var count: Int = 0

      while (!task.idle) {
        count += 1
        if (count > 500) {
          sys.error("Taking forever!")
        }
        Thread.sleep(10)
      }
    }
  }
}
