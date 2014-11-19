package com.monovore.integration.coast

import com.monovore.coast
import com.monovore.coast.format.WireFormat
import com.monovore.coast.samza.CoastKafkaSystem
import kafka.producer.{ProducerConfig, Producer}
import org.apache.samza.system.{SystemStream, OutgoingMessageEnvelope}
import org.specs2.ScalaCheck
import org.specs2.mutable._

class CoastKafkaSystemSpec extends Specification with ScalaCheck {

  val BigNumber = 5000

  sequential

  "a ratcheted kafka producer" should {

    import coast.format.pretty._

    def stream(name: String) = coast.Name[String, Int](name)

    "eventually write all the data" in {

      IntegrationTest.withKafkaCluster { props =>

        IntegrationTest.slurp(Set("test"), props)

        val producer = new CoastKafkaSystem.Producer(
          new Producer(new ProducerConfig(props)),
          {
            case "odd" => 1
            case "even" => 2
            case _ => 0
          }
        )

        producer.register("producer-test")

        producer.start()

        val messages = for (i <- 1 to BigNumber) yield {
          new OutgoingMessageEnvelope(
            new SystemStream("producer-test", "test"),
            "empty".##,
            WireFormat.write("empty"),
            WireFormat.write(i)
          )
        }

        for (message <- messages) {
          producer.send("producer-test", message)
        }

        producer.flush("producer-test")

        Thread.sleep(5000)

        producer.stop()

        val returned = IntegrationTest.slurp(Set("test"), props)

        returned.get(stream("test")).apply("empty") must_== (1 to BigNumber)
      }
    }

    "maintain some ordering invariants" in {

      IntegrationTest.withKafkaCluster { clientProps =>

        IntegrationTest.slurp(Set("even", "odd"), clientProps)

        val producer = new CoastKafkaSystem.Producer(
          new Producer(new ProducerConfig(clientProps)),
          {
            case "odd" => 1
            case "even" => 2
            case _ => 0
          }
        )

        producer.register("producer-test")

        producer.start()

        val messages = for (i <- 1 to BigNumber) yield {
          new OutgoingMessageEnvelope(
            new SystemStream("producer-test", if (i % 2 == 0) "even" else "odd"),
            "empty".##,
            WireFormat.write("empty"),
            WireFormat.write(i)
          )
        }

        for (message <- messages) {
          producer.send("producer-test", message)
          Thread.sleep(0, 500)
        }

        producer.stop()

        val persisted = IntegrationTest.slurp(Set("even", "odd"), clientProps)

        def contiguous(seq: Seq[Int]) = seq.grouped(2).foreach {
          case Seq(a, b) => { (b - a) must_== 2 }
          case _ => {}
        }

        val evens = persisted.get(stream("even")).apply("empty")
        val odds = persisted.get(stream("odd")).apply("empty")

        val allOdds = odds.toSet

        evens.foreach { even => allOdds.contains(even - 1) must_== true }

        contiguous { evens }
        contiguous { odds }

        evens must not be empty
        odds must not be empty
      }
    }
  }
}
