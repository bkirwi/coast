package com.monovore.coast.standalone.kafka

import java.util

import com.monovore.coast.core._
import org.apache.kafka.clients.consumer.internals.PartitionAssignor
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.{Assignment, Subscription}
import org.apache.kafka.common.{TopicPartition, Cluster, Configurable}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class CoastAssignor extends PartitionAssignor with Configurable {

  private[this] val logger = LoggerFactory.getLogger(getClass)

  import CoastAssignor._

  var whatever: Map[TopicName, Set[TopicName]] = _

  override def configure(configs: util.Map[String, _]): Unit = {

    val KeyRegex = """coast\.topics\.(.+)""".r

    whatever =
      configs.asScala
        .collect { case (KeyRegex(key), value: String) => key -> value.split(",").toSet }
        .toMap
  }

  override val name: String = "coast"

  override def subscription(topics: util.Set[TopicName]): Subscription =
    new Subscription(topics.asScala.toSeq.asJava)

  override def assign(
    metadata: Cluster,
    subscriptions: util.Map[MemberID, Subscription]
  ): util.Map[MemberID, Assignment] = {

    val assignments = partitionAssignments(whatever, { topic => metadata.partitionCountForTopic(topic)})

    logger.info(assignments.toString)

    val membersForTask =
      whatever
        .map { case (task, sources) =>
          val members =
            subscriptions.asScala
              .filter { case (member, subscription) =>
                subscription.topics.contains(task) && sources.forall(subscription.topics.contains)
              }
              .keySet

          task -> members
        }

    println(membersForTask)

    val topicsForMember =
      assignments.toSeq
        .flatMap { case (log, sources) =>
          val memberOpt = // an arbitrary but ~consistent choice, if there is one
            membersForTask.get(log.topic)
              .filter { _.nonEmpty }
              .map { set => set.toIndexedSeq.sorted.apply(log.hashCode % set.size) }

          for (partition <- sources + log; member <- memberOpt) yield member -> partition
        }
        .groupBy { _._1 }
        .mapValues { _.map { _._2 } }

    println(topicsForMember)

    subscriptions.asScala
      .map { case (k, _) => k -> new Assignment(topicsForMember.getOrElse(k, Seq.empty).asJava) }
      .asJava
  }

  override def onAssignment(assignment: Assignment): Unit = {}
}

object CoastAssignor {

  type TopicName = String
  type MemberID = String

  def topicGroups(graph: Graph): Map[TopicName, Set[TopicName]] = {

    def sources[A, B](node: Node[A, B]): Set[TopicName] = node match {
      case Source(name) => Set(name)
      case Transform(upstream, _, _) => sources(upstream)
      case Merge(all) => all.flatMap { case (k, v) => sources(v) }.toSet
      case GroupBy(upstream, _) => sources(upstream)
    }

    graph.bindings
      .map { case (name, sink) => s"coast.log.$name" -> sources(sink.element) }
      .toMap
  }

  def partitionAssignments(
    topics: Map[TopicName, Set[TopicName]],
    partitionCounts: TopicName => Int
  ): Map[TopicPartition, Set[TopicPartition]] =

    topics
      .flatMap { case (log, sources) =>
        val count = partitionCounts(log)

        for (source <- sources) {
          require(partitionCounts(source) == count, "Source must have the same number of partitions as the task log.")
        }

        for (i <- 0 until count) yield {
          new TopicPartition(log, i) -> sources.map { new TopicPartition(_, i) }
        }
      }
}
