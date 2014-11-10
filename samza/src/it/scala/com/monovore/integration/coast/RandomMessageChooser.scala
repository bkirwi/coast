package com.monovore.integration.coast

import org.apache.samza.config.Config
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.system.chooser.{BaseMessageChooser, MessageChooser, MessageChooserFactory}

import scala.util.Random

class RandomMessageChooser extends BaseMessageChooser {

  var messages: List[IncomingMessageEnvelope] = Nil

  override def update(p1: IncomingMessageEnvelope): Unit = { messages = p1 :: messages }

  override def choose(): IncomingMessageEnvelope = {
    if (messages.isEmpty || Random.nextDouble() < 0.2) null
    else {
      val (head :: tail) = Random.shuffle(messages)
      messages = tail
      head
    }
  }
}

class RandomMessageChooserFactory extends MessageChooserFactory {
  override def getChooser(p1: Config, p2: MetricsRegistry): MessageChooser = new RandomMessageChooser
}
