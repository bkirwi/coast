package com.monovore.coast.samza

case class Path(root: String, branches: Seq[String] = Vector.empty, index: Int = 0) {

  def /(subpath: String): Path = copy(branches = branches :+ subpath, index = 0)

  def next: Path = copy(index = index + 1)

  override def toString: String = {
    val suffix = Seq(index).filter { _ != 0 }.map { _.toString }
    ((root +: branches) ++ suffix).mkString(".")
  }
}
