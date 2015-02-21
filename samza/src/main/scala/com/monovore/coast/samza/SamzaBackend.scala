package com.monovore.coast.samza

import org.apache.samza.config.Config
import org.apache.samza.system.SystemFactory

import collection.JavaConverters._

trait SamzaBackend extends (Config => ConfigGenerator)

object SamzaBackend {

  def getPartitions(config: Config, system: String, stream: String) = {

    val systemFactory = config.getNewInstance[SystemFactory](s"systems.$system.samza.factory")
    val admin = systemFactory.getAdmin(system, config)
    val meta = admin.getSystemStreamMetadata(Set(stream).asJava).asScala
      .getOrElse(stream, sys.error(s"Couldn't find metadata on output stream $stream"))

    val partitionMeta = meta.getSystemStreamPartitionMetadata.asScala

    partitionMeta.size
  }
}