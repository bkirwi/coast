package com.monovore.coast
package mini

import com.monovore.coast.machine.{Key, Message}
import org.specs2.ScalaCheck
import org.specs2.mutable._

class ClusterSpec extends Specification with ScalaCheck {

  "a mini-cluster" should {

    "behave correctly" in {

      val source = Name[String, Int]("source")
      val sink = Name[String, Int]("sink")

      val flow = for {
        _ <- Flow.sink(sink) {
          Flow.source(source).fold(0) { _ + _ }.stream
        }
      } yield ()

      val cluster = new Cluster()

      val input = (1 to 500).map { n => s"great-${n % 4}" -> n }

      cluster.send(source, input: _*)

      for (_ <- 1 to 6) {
        cluster.whileRunning(flow) { _ => Thread.sleep(10) }
      }

      cluster.whileRunning(flow) { running =>
        running.complete()
      }

      val output = cluster.messages(sink)(sink)

      output must_== input.groupByKey.mapValues { _.scanLeft(0) { _ + _ }.tail }
    }
  }
}
