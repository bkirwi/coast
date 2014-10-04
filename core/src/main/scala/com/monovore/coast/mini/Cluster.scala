package com.monovore.coast
package mini

import java.util.concurrent.{Executors, LinkedBlockingQueue, BlockingQueue}

import com.monovore.coast.machine.{Messages, Key, Message}

sealed trait Thing[A, B]
case class Data[A, B](pair: A -> B) extends Thing[A, B]
case class Offset[A, B](offset: Int) extends Thing[A, B]

class Cluster(log: Log[String, Key -> Message]) {

  def this() { this(Log.empty()) }

  def send[A, B](name: Name[A, B], messages: (A -> B)*): Unit = {
    messages
      .map { case (k, v) => Key(k) -> Message(v) }
      .foreach { pair => log.push(name.name, pair) }
  }

  def output(names: Name[_, _]*): Messages = Messages(
    names
      .map { name =>
        name.name -> log.fetchAll(name.name).groupByKey
      }
      .toMap
  )

  trait Task {
    @volatile var idle: Boolean = false
    def step(): Unit
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
      queue: BlockingQueue[Thing[A, B]]
    ): Seq[Task] = element match {
      case Source(name) => {
        val task = new Task {

          private[this] var offset: Int = 0

          def step(): Unit = {

            log.fetch(name, offset) match {
              case Some(k -> v) => {
                queue.put(Offset(offset))
                queue.put(Data(k.cast -> v.cast))
                offset += 1
                idle = false
              }
              case None => {
                idle = true
              }
            }
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

            override def step(): Unit = {

              val next = Option(upQueue.poll())

              next foreach {
                case Data(k -> msg) => {
                  val (newState, out) = transformer(k)(state(k), msg)
                  state += (k -> newState)
                  out.foreach { msg => queue.put(Data(k -> msg)) }
                }
                case Offset(o) => {
                  queue.put(Offset(o))
                }
              }

              idle = next.isEmpty
            }
          }

          upstreamTask :+ myTask
        }

        doFor(queue, upstream, init, transformer)
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

        private[this] var offset: Int = myCheckpoint

        private[this] val skipUntil: Int = log.fetchAll(sink).size

        def step(): Unit = {

          val next = Option(queue.poll())

          next foreach {
            case Data(k -> msg) => {
              if (offset >= skipUntil) log.push(sink, Key(k) -> Message(msg))
              offset += 1
            }
            case Offset(o) => {
              log.push(checkpoints, Key(unit) -> Message(o -> offset))
            }
          }

          idle = next.isEmpty
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
          task.step()
//          Thread.sleep(4)
          executor.submit(this)
        }
      }

      executor.submit(runnable)
    }


    def stop(): Unit = executor.shutdownNow()

    def complete(): Unit = tasks foreach { task =>

      while (!task.idle) Thread.sleep(10)

    }

//    class Consumer(input: String, output: BlockingQueue[Key -> Message]) extends Runnable {
//
//      @volatile private[this] var offset: Int = 0
//
//      def isComplete(): Boolean = log.fetch(input, offset).isEmpty
//
//      override def run(): Unit = {
//
//        while (true) {
//
//          for {
//            next <- log.fetch(input, offset)
//          } {
//            output.put(next)
//            offset += 1
//          }
//        }
//      }
//    }
//
//    class Processor(input: BlockingQueue[Key -> Message], process: (Key -> Message) => Unit) extends Runnable {
//
//      override def run(): Unit = {
//
//        while (true) {
//          val next = input.take()
//          process(next)
//        }
//      }
//    }
  }
}
