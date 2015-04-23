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
    warn(s"Ignoring checkpoint for $taskName; you probably want to disable checkpointing for this job.")
  }

  override def readLastCheckpoint(taskName: TaskName): SamzaCheckpoint =
    new SamzaCheckpoint(util.Collections.emptyMap())

  override def writeChangeLogPartitionMapping(mapping: util.Map[TaskName, Integer]): Unit = {}

  override def readChangeLogPartitionMapping(): util.Map[TaskName, Integer] = {

    val maxPartitions = 1000

    info(s"Returning partition mapping for $maxPartitions static partitions. If you have more than $maxPartitions partitions, this might not work!")

    val map = new util.HashMap[TaskName, Integer]()

    for (i <- 0 until 100) {
      map.put(new TaskName(s"Partition $i"), i)
    }

    map
  }
}

class StaticCheckpointManagerFactory extends CheckpointManagerFactory {
  override def getCheckpointManager(config: Config, registry: MetricsRegistry): CheckpointManager = new StaticCheckpointManager
}
