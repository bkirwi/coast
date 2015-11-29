package com.monovore.coast.standalone

import java.util.concurrent.Executors

import com.lmax.disruptor.{Sequence, EventTranslator, RingBuffer, EventFactory}
import com.lmax.disruptor.dsl.Disruptor
import org.specs2.mutable._
import org.specs2.ScalaCheck

class DisruptorSpec extends Specification with ScalaCheck {

  case class Ref(var value: Long)

  object Ref extends EventFactory[Ref] {
    override def newInstance(): Ref = Ref(6)
  }

  case class Pick(var right: Boolean, var upTo: Long)

  object Pick extends EventFactory[Pick] {
    override def newInstance(): Pick = Pick(false, 0)
  }

  "DisruptorSpec" should {

    "pass a test" in {

      val left = RingBuffer.createSingleProducer(Ref, 1024)
      val right = RingBuffer.createSingleProducer(Ref, 1024)
      val picks = RingBuffer.createSingleProducer(Pick, 1024)

      val picksBarrier = picks.newBarrier()

      val picked = new Sequence(-1L)

      for (i <- 1 to 100) {
        left.publishEvent(new EventTranslator[Ref] {
          override def translateTo(event: Ref, sequence: Long): Unit = {
            event.value = i
          }
        })

        right.publishEvent(new EventTranslator[Ref] {
          override def translateTo(event: Ref, sequence: Long): Unit = {
            event.value = i
          }
        })

        picks.publishEvent(new EventTranslator[Pick] {
          override def translateTo(event: Pick, sequence: Long): Unit = {
            event.right = (i % 2 == 0)
            event.upTo = i
          }
        })
      }

      val upTo = picksBarrier.waitFor(0L)

      var next = picked.get() + 1

      while (next <= upTo) {
        val got = picks.get(next)

        val toPoll =

        next += 1
      }

      true
    }
  }
}
