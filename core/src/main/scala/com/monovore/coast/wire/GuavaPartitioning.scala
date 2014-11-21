package com.monovore.coast.wire

import com.google.common.base.Charsets
import com.google.common.hash.{Funnel, Funnels, HashFunction}

trait GuavaPartitioning {

  protected def guavaHashFunction: HashFunction

  implicit val intPartitioner: Partitioner[Int] =
    GuavaPartitioner(guavaHashFunction, Funnels.integerFunnel().asInstanceOf[Funnel[Int]])

  implicit val stringPartitioner: Partitioner[String] =
    GuavaPartitioner(guavaHashFunction, Funnels.stringFunnel(Charsets.UTF_8))

  implicit val longPartitioner: Partitioner[Long] =
    GuavaPartitioner(guavaHashFunction, Funnels.longFunnel().asInstanceOf[Funnel[Long]])
}
