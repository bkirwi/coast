package com.monovore.coast

import com.google.common.primitives.{Ints, Longs}
import com.monovore.coast.wire.BinaryFormat
import com.monovore.coast.model._
import org.apache.samza.config.{Config, MapConfig}

import scala.collection.JavaConverters._

package object samza {

  val TaskKey = "coast.task.serialized.base64"
  val TaskName = "coast.task.name"
  val RegroupedStreams = "coast.streams.regrouped"

  val CoastSystem = "coast-system"

  private[samza] def formatPath(path: List[String]): String = {
    if (path.isEmpty) "."
    else path.reverse.mkString(".")
  }

  def config(pairs: (String -> String)*): Config = new MapConfig(pairs.toMap.asJava)
}
