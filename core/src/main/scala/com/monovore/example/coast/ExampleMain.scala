package com.monovore.example.coast

import java.io.File

import com.google.common.base.Charsets
import com.google.common.io.Files
import com.monovore.coast.viz.Dot
import com.monovore.coast.model.Graph

/**
 * A simple main method for running the example jobs. At the moment, it just
 * pretty-prints the flows in GraphViz format.
 */
trait ExampleMain {

  def graph: Graph

  def main(args: Array[String]): Unit = {

    args.toList match {
      case List("dot") => println(Dot.describe(graph))
      case List("dot", path) => {
        Files.asCharSink(new File(path), Charsets.UTF_8).write(Dot.describe(graph))
      }
    }
  }
}
