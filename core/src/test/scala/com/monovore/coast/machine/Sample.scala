package com.monovore.coast
package machine

import org.scalacheck.Gen

import scala.annotation.tailrec
import scala.util.Random

object Sample {

  def complete(machine: Machine): Gen[Messages] = {

    @tailrec def doComplete(machine: Machine, messages: Messages): Messages = {

      val next = Random.shuffle(machine.next).headOption

      next match {
        case Some(shit) => {
          val (newMachine, newMessages) = shit()
          doComplete(newMachine, messages ++ newMessages)
        }
        case None => messages
      }
    }

    Gen.wrap { doComplete(machine, Messages.empty) }
  }

  def canProduce(machine: Machine, output: Messages): Boolean = {

    def dropSeq[A](prefix: Seq[A], other: Seq[A]): Option[Seq[A]] = {
      val (left, right) = other.splitAt(prefix.length)
      assuming (left == prefix) { right }
    }

    def dropMap[A, B](dropValuePrefix: (B, B) => Option[B])(prefix: Map[A, B], other: Map[A, B]): Option[Map[A, B]] = {

      prefix.foldLeft(Some(other): Option[Map[A, B]]) { (acc, kv) =>

        val (key, valuePrefix) = kv

        for {
          accMap <- acc
          value <- accMap.get(key)
          dropped <- dropValuePrefix(valuePrefix, value)
        } yield accMap.updated(key, dropped)
      }
    }


    val next = machine.next

    val result =
      if (next.isEmpty) output.isEmpty
      else {
        next.exists { case nextFn =>

          val (machin, soFar) = nextFn()

          val dropped = dropMap(dropMap[Key, Seq[Message]](dropSeq[Message]))(soFar.messageMap, output.messageMap)

          dropped.exists { rest =>
            canProduce(machin, Messages(rest))
          }
        }
      }

    result
  }
}
