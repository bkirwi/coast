package com.monovore.coast
package machine

import org.scalacheck.Gen

object Sample {

  def complete(machine: Machine): Gen[Messages] = {

    val next = machine.next

    if (next.isEmpty) Gen.const(Messages.empty)
    else {
      for {
        (machine, output) <- Gen.oneOf(next.toSeq)
        finish <- complete(machine)
      } yield output ++ finish
    }
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
        next.exists { case (machin, soFar) =>

          val dropped = dropMap(dropMap[Key, Seq[Message]](dropSeq[Message]))(soFar.messageMap, output.messageMap)

          dropped.exists { rest =>
            canProduce(machin, Messages(rest))
          }
        }
      }

    result
  }
}
