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
          Flow.merge(Flow.source(source), Flow.source(source))
        }
      } yield ()

      val inputSize = 200

      val input = (1 to inputSize).map { n => s"great-${n % 4}" -> n }

      val actors = new Cluster(Log.empty())

      actors.send(source, input: _*)

      for (i <- 1 to 6) {
        actors.withRunning(flow) { system =>
          Thread.sleep(4)
        }
      }

      actors.withRunning(flow) { system =>
        Thread.sleep(1000)
      }

      val output = actors.messages(sink)(sink)

      output.values.map { _.size }.sum must_== (inputSize * 2)
    }
  }
}
