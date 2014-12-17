package com.monovore.example.coast

import com.monovore.coast
import com.monovore.coast.flow

object EntityResolution extends ExampleMain {
  
  import coast.wire.ugly._

  type Scope = Int
  type SourceID = Int
  case class Entity()
  
  def scope(entity: Entity): Seq[Scope] = ???
  
  val RawEntities = flow.Name[SourceID, Entity]("raw-entities")

  val graph = for {
    
    input <- flow.stream("input") {
      flow.source(RawEntities)
        .flatMap { e => scope(e).map { _ -> e }}
        .groupByKey
    }

    got <- flow.cycle[Scope, Entity]("dangerous") { merged =>

      val entities = flow.merge("self" -> merged, "fresh" -> input)

      entities
        .aggregate(Set.empty[Entity]) { (set, next) =>
          (set + next) -> Seq.empty[Entity]
        }
    }
  } yield ()
}
