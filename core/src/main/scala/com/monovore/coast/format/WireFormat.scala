package com.monovore.coast.format

import scala.annotation.implicitNotFound


@implicitNotFound("No implicit wire format found for type ${A}. Try importing something from coast.format, or define your own!")
trait WireFormat[A] extends Serializable {
  def write(value: A): Array[Byte]
  def read(bytes: Array[Byte]): A
}
