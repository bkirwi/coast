package com.monovore.integration.coast

import com.monovore.coast.wire.Protocol
import org.scalacheck.Gen
import org.specs2.ScalaCheck
import org.specs2.mutable._

class SimpleIntegrationSpec extends Specification with ScalaCheck {

  sequential

  "a 'simple' samza-backed job" should {

    "count words" in {

      import com.monovore.example.coast.WordCount._
      import Protocol.simple._

      val words = Gen.oneOf("testing", "scandal", "riviera", "salad", "Thursday")
      val sentences = Gen.listOf(words).map { _.mkString(" ") }
      val sentenceList = Seq.fill(100)(sentences.sample).flatten.toSeq

      val input = Messages.add(Sentences, Map(0L -> sentenceList))

      val output = IntegrationTest.fuzz(graph, input, simple = true).get(WordCounts)

      val testingCount =
        sentenceList
          .flatMap { _.split(" ") }
          .count { _ == "testing" }

      output("testing") must_== (1 to testingCount)
    }
  }
}
