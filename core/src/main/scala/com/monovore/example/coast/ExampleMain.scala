package com.monovore.example.coast

import java.io.File

import com.google.common.base.Charsets
import com.google.common.io.Files
import com.monovore.coast
import com.monovore.coast.Flow

/**
 * A simple main method for running the example jobs. At the moment, it just pretty-prints
 * the flows in GraphViz format.
 */
trait ExampleMain {

  def flow: Flow[Unit]

  def main(args: Array[String]): Unit = {

    args.toList match {
      case List("dot") => println(coast.dot.Dot(flow))
      case List("dot", path) => {
        Files.asCharSink(new File(path), Charsets.UTF_8).write(coast.dot.Dot(flow))
      }
    }
  }
}
