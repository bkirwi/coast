package com.monovore.coast
package samza

import org.apache.samza.config.Config
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system.{SystemStreamPartition, IncomingMessageEnvelope}
import org.apache.samza.system.chooser.{MessageChooserFactory, MessageChooser}

class CoastMessageChooser extends MessageChooser {

  override def start(): Unit = {}

  override def register(p1: SystemStreamPartition, p2: String): Unit = {}

  override def stop(): Unit = {}

  override def update(p1: IncomingMessageEnvelope): Unit = {}

  override def choose(): IncomingMessageEnvelope = null
}

object CoastMessageChooser {

  class Factory extends MessageChooserFactory {

    override def getChooser(p1: Config, p2: MetricsRegistry): MessageChooser =
      new CoastMessageChooser
  }
}
