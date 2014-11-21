package com.monovore.coast.wire

import scala.annotation.implicitNotFound


@implicitNotFound("No implicit wire format found for type ${A}. Try importing something from coast.format, or define your own!")
trait WireFormat[A] extends Serializable {
  def write(value: A): Array[Byte]
  def read(bytes: Array[Byte]): A
}

object WireFormat {
  def write[A](value: A)(implicit fmt: WireFormat[A]): Array[Byte] = fmt.write(value)
  def read[A](bytes: Array[Byte])(implicit fmt: WireFormat[A]): A = fmt.read(bytes)
}
