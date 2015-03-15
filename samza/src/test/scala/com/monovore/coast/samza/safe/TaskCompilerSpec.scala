package com.monovore.coast.samza.safe

import com.monovore.coast.flow.{Flow, Topic}
import com.monovore.coast.samza.Path
import com.monovore.coast.wire.BinaryFormat
import org.specs2.ScalaCheck
import org.specs2.mutable._

class TaskCompilerSpec extends Specification with ScalaCheck {

  "a task compiler" should {

    import com.monovore.coast.wire.pretty._

    val compiler = new TaskCompiler(new TaskCompiler.Context {
      override def getStore[A, B](path: String, default: B): CoastState[A, B] = new CoastState[A, B] {

        var _upstream: Long = 0L
        var _downstream: Long = 0L
        var _state: Map[A, B] = Map.empty[A, B]

        override def upstream: Long = _upstream
        override def downstream: Long = _downstream
        override def state(key: A): B = _state.getOrElse(key, default)
        override def push(key: A, value: B, upstream: Long, downstream: Long): Unit = {
          _upstream = upstream
          _downstream = downstream
          _state += (key -> value)
        }
      }
    })

    class Capture[A, B] extends ((Long, A, B) => Long) {

      var captured: Seq[(A, B)] = Vector.empty
      var highWater: Long = 0L

      def apply(offset: Long, key: A, value: B) = {

        if (offset >= highWater) {
          captured :+= (key -> value)
          highWater = offset + 1
        }

        highWater
      }
    }

    "compile a simple branch" in {

      val Input = Topic[String, Int]("input")

      val branch = Flow.source(Input).flatMap { x => Seq(x, x) }.element

      val sink = new Capture[String, Int]

      val Seq(compiled) = compiler.compile(branch, sink, "output")

      compiled.inputStream must_== Input.name
      compiled.downstreamPath must_== Path("output")

      compiled.handler(0, BinaryFormat.write("car"), BinaryFormat.write(3))
      sink.captured must_== Seq("car" -> 3, "car" -> 3)
    }

    "compile a stateful branch" in {

      val Input = Topic[String, Int]("input")

      val branch = Flow.source(Input).fold(0) { _ + _ }.updates.element

      val sink = new Capture[String, Int]

      val Seq(compiled) = compiler.compile(branch, sink, "output")

      compiled.inputStream must_== Input.name
      compiled.downstreamPath must_== Path("output").next

      compiled.handler(0, BinaryFormat.write("car"), BinaryFormat.write(3))
      sink.captured must_== Seq("car" -> 3)

      compiled.handler(0, BinaryFormat.write("car"), BinaryFormat.write(3))
      sink.captured must_== Seq("car" -> 3)

      compiled.handler(1, BinaryFormat.write("car"), BinaryFormat.write(3))
      sink.captured must_== Seq("car" -> 3, "car" -> 6)

      compiled.handler(2, BinaryFormat.write("dog"), BinaryFormat.write(3))
      sink.captured must_== Seq("car" -> 3, "car" -> 6, "dog" -> 3)
    }

    "compile a merged branch" in {

      val First = Topic[String, Int]("first")
      val Second = Topic[String, Int]("second")

      val branch = Flow.merge(
        "first" -> Flow.source(First),
        "second" -> Flow.source(Second)
      )

      val sink = new Capture[String, Int]

      val Seq(first, second) = compiler.compile(branch.element, sink, "output")

      first.inputStream must_== First.name
      first.downstreamPath must_== Path("output")

      second.inputStream must_== Second.name
      second.downstreamPath must_== Path("output")

      first.handler(0, BinaryFormat.write("car"), BinaryFormat.write(1))
      second.handler(0, BinaryFormat.write("car"), BinaryFormat.write(2))

      sink.captured must_== Seq("car" -> 1, "car" -> 2)
    }
  }
}
