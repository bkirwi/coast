package com.monovore.coast.samza.safe

import java.util

import org.apache.samza.checkpoint.{Checkpoint, CheckpointManager, CheckpointManagerFactory}
import org.apache.samza.config.Config
import org.apache.samza.container.TaskName
import org.apache.samza.metrics.MetricsRegistry

class NoopCheckpointManager extends CheckpointManager {

  override def start(): Unit = {}

  override def stop(): Unit = {}

  override def register(taskName: TaskName): Unit = {}

  override def writeCheckpoint(taskName: TaskName, checkpoint: Checkpoint): Unit = {}

  override def readLastCheckpoint(taskName: TaskName): Checkpoint =
    new Checkpoint(util.Collections.emptyMap())

  override def writeChangeLogPartitionMapping(mapping: util.Map[TaskName, Integer]): Unit = {}

  override def readChangeLogPartitionMapping(): util.Map[TaskName, Integer] = {

    val map = new util.HashMap[TaskName, Integer]()

    for (i <- 0 until 10) {
      map.put(new TaskName(s"Partition $i"), i)
    }

    map
  }
}

class NoopCheckpointManagerFactory extends CheckpointManagerFactory {
  override def getCheckpointManager(config: Config, registry: MetricsRegistry): CheckpointManager = new NoopCheckpointManager
}
