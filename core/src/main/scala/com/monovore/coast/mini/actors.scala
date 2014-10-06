package com.monovore.coast
package mini

import akka.actor.{ActorRef, Actor}

case class Checkpoint(data: Any)
case object Readey
case object Next
case class NewMessage(data: Any)

class SpoutActor(spout: String, log: Log[String, Any]) extends Actor { actor =>

  var offset: Int = 0

  override def receive = bootstrap

  def bootstrap: Receive = {
    case Checkpoint(offset: Int) => {
      actor.offset = offset
    }
    case Readey => {
      context.become(process(context.sender()))
      context.self ! Next
    }
  }

  def process(downstream: ActorRef): Receive = {

    case Next => {

      val next = log.fetch(spout, actor.offset)

      next foreach { data =>
        downstream ! NewMessage(data)
        context.self ! Next
      }
    }
  }
}