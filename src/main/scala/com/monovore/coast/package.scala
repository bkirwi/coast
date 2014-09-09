package com.monovore

package object coast {

  implicit class SeqOps[A](underlying: Seq[A]) {
    def groupByKey[B,C](implicit proof: A <:< (B, C)): Map[B, Seq[C]] =
      underlying.groupBy { _._1 }.mapValues { _.unzip._2 }
  }
}
