package com.monovore.coast.format

trait WireFormat[A] extends Serializable {
  def write(value: A): Array[Byte]
  def read(bytes: Array[Byte]): A
}
