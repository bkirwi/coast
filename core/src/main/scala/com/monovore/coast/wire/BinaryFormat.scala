package com.monovore.coast.wire

import scala.annotation.implicitNotFound


@implicitNotFound("No byte format in scope for type ${A}")
trait BinaryFormat[A] extends Serializable {
  def write(value: A): Array[Byte]
  def read(bytes: Array[Byte]): A
}

object BinaryFormat {
  def write[A](value: A)(implicit fmt: BinaryFormat[A]): Array[Byte] = fmt.write(value)
  def read[A](bytes: Array[Byte])(implicit fmt: BinaryFormat[A]): A = fmt.read(bytes)
}
