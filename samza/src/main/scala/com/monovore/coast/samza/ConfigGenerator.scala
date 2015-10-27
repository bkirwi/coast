package com.monovore.coast
package samza

import com.monovore.coast.core._
import org.apache.samza.config.Config
import wire.Serializer

import scala.language.existentials

trait ConfigGenerator {
  def configure(graph: Graph): Map[String, Config]
}

object ConfigGenerator {

  def isRegrouped[A, B](node: Node[A, B]): Boolean = node match {
    case _: Source[_, _] => false
    case clock: Clock => false
    case trans: Transform[_, _, _, _] => isRegrouped(trans.upstream)
    case merge: Merge[_, _] => merge.upstreams.exists { case (_, up) => isRegrouped(up) }
    case group: GroupBy[_, _, _] => true
  }

  def storageFor[A, B](element: Node[A, B], path: Path): Map[Path, Storage] = element match {
    case Source(_) => Map.empty
    case clock: Clock => Map.empty
    case PureTransform(up, _) => storageFor(up, path)
    case Merge(ups) => {
        ups.flatMap { case (branch, up) => storageFor(up, path / branch)}.toMap
    }
    case agg @ StatefulTransform(up, _, _) => {
        val upstreamed = storageFor(up, path.next)
        val storage = Storage(path.toString, agg.keyFormat, agg.stateFormat)
        upstreamed.updated(path, storage)
    }
    case GroupBy(up, _) => storageFor(up, path)
  }

  def sourcesFor[A, B](element: Node[A, B]): Set[String] = element match {
    case Source(name) => Set(name)
    case clock: Clock => sys.error("Clocks not implemented yet!")
    case Transform(up, _, _) => sourcesFor(up)
    case Merge(ups) => ups.flatMap { case (_, up) => sourcesFor(up) }.toSet
    case GroupBy(up, _) => sourcesFor(up)
  }

  case class Storage(name: String, keyString: Serializer[_], valueString: Serializer[_])
}
