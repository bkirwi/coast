package com.monovore.coast.standalone.kafka

import java.util

import com.monovore.coast.core._
import org.apache.kafka.clients.consumer.internals.PartitionAssignor
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.{Assignment, Subscription}
import org.apache.kafka.common.{TopicPartition, Cluster, Configurable}

import scala.collection.JavaConverters._

class CoastAssignor extends PartitionAssignor with Configurable {

  type Topic = String
  type MemberID = String

  var whatever: Map[Topic, Seq[Topic]] = _

  override def configure(configs: util.Map[String, _]): Unit = {

    val KeyRegex = """coast\.topics\.(.+)""".r

    whatever =
      configs.asScala
        .collect { case (KeyRegex(key), value: String) => key -> value.split(",").toSeq }
        .toMap
  }

  override val name: String = "coast"

  override def subscription(topics: util.Set[Topic]): Subscription =
    new Subscription(topics.asScala.toSeq.asJava)

  override def assign(
    metadata: Cluster,
    subscriptions: util.Map[MemberID, Subscription]
  ): util.Map[MemberID, Assignment] = {

    println(whatever)

    for ((key, subs) <- subscriptions.asScala) {
      subs.topics.asScala.foreach { topic =>
        require(whatever.exists { case (k, v) => k == topic || v.contains(topic)}, s"Missing $topic") }
    }

    val topixx =
      whatever
        .map { case (topix, values) =>
            val x: Int = metadata.partitionCountForTopic(topix)

            for (v <- values) {
              require(x == metadata.partitionCountForTopic(v))
            }

            for {
              topic <- topix +: values
              i <- 0 until x
            } yield new TopicPartition(topic, i)
        }

    println(topixx)

    topixx.zipWithIndex
      .groupBy { _._2 % subscriptions.size }
      .map { case (_, all) => new Assignment(all.flatMap { _._1 }.toSeq.asJava) }
      .zip(subscriptions.keySet.asScala)
      .map { _.swap }
      .toMap
      .asJava
  }

  override def onAssignment(assignment: Assignment): Unit = {}
}

object CoastAssignor {

  def topicGroups(graph: Graph): Map[String, Set[String]] = {

    def sources[A, B](node: Node[A, B]): Set[String] = node match {
      case Source(name) => Set(name)
      case Transform(upstream, _, _) => sources(upstream)
      case Merge(all) => all.flatMap { case (k, v) => sources(v) }.toSet
      case GroupBy(upstream, _) => sources(upstream)
    }


    graph.bindings
      .map { case (name, sink) => s"coast.backing.$name" -> sources(sink.element) }
      .toMap
  }
}
