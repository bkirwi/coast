package com.monovore.integration.coast

import org.apache.samza.config.Config
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.system.chooser.{BaseMessageChooser, MessageChooser, MessageChooserFactory}
import org.apache.samza.util.Logging

import scala.util.Random

class RandomMessageChooser extends BaseMessageChooser with Logging {

  private[this] val token = Random.alphanumeric.take(20).mkString

  var messages: List[IncomingMessageEnvelope] = Nil

  override def update(p1: IncomingMessageEnvelope): Unit = {

    trace(s"$token: New message from ${p1.getSystemStreamPartition}")

    messages = p1 :: messages
  }

  override def choose(): IncomingMessageEnvelope = {
    if (messages.isEmpty || Random.nextDouble() < 0.2) {
      trace(s"$token: Wanted messages, but I'm out")
      null
    }
    else {
      val (head :: tail) = Random.shuffle(messages)
      messages = tail
      trace(s"$token: Choosing message from ${head.getSystemStreamPartition}")
      head
    }
  }
}

class RandomMessageChooserFactory extends MessageChooserFactory {
  override def getChooser(p1: Config, p2: MetricsRegistry): MessageChooser = new RandomMessageChooser
}
