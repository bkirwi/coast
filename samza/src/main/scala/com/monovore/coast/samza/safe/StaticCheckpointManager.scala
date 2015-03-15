package com.monovore.coast.samza.safe

import java.util

import org.apache.samza.checkpoint.{Checkpoint => SamzaCheckpoint, CheckpointManager, CheckpointManagerFactory}
import org.apache.samza.config.Config
import org.apache.samza.container.TaskName
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.util.Logging

class StaticCheckpointManager extends CheckpointManager with Logging {

  override def start(): Unit = {}

  override def stop(): Unit = {}

  override def register(taskName: TaskName): Unit = {}

  override def writeCheckpoint(taskName: TaskName, checkpoint: SamzaCheckpoint): Unit = {
    warn(s"Ignoring checkpoint for $taskName; you may want to disable checkpointing or use a real checkpoint manager")
  }

  override def readLastCheckpoint(taskName: TaskName): SamzaCheckpoint =
    new SamzaCheckpoint(util.Collections.emptyMap())

  // FIXME: review / document this

  override def writeChangeLogPartitionMapping(mapping: util.Map[TaskName, Integer]): Unit = {}

  override def readChangeLogPartitionMapping(): util.Map[TaskName, Integer] = {

    val map = new util.HashMap[TaskName, Integer]()

    for (i <- 0 until 100) {
      map.put(new TaskName(s"Partition $i"), i)
    }

    map
  }
}

class NoopCheckpointManagerFactory extends CheckpointManagerFactory {
  override def getCheckpointManager(config: Config, registry: MetricsRegistry): CheckpointManager = new StaticCheckpointManager
}
