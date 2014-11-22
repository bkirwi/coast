package com.monovore.coast.samza

import org.specs2.mutable._
import org.specs2.ScalaCheck
import com.monovore.coast

class CompilationSpec extends Specification with ScalaCheck {

  case class Captured[A, B](stream: String, partition: Int, offset: Long, key: A, value: B)

  class CapturingSink[A, B](var captured: Seq[Captured[A, B]] = Seq.empty) extends MessageSink[A, B] {

    override def execute(stream: String, partition: Int, offset: Long, key: A, value: B): Long = {

      captured :+= Captured(stream, partition, offset, key, value)

      offset + 1
    }
  }

  "CompilationSpec" should {

    "pass a test" in {

//      import coast.wire.pretty._
//
//      val compiler = new Compiler(null)
//
//      val sink = new CapturingSink[String, Int]()
//
//      val Source = coast.Name[String, Int]("source")
//
//      compiler.compile(coast.source(Source).element, sink, Nil)

      true
    }
  }
}
