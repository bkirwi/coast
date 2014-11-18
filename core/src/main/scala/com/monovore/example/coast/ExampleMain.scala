package com.monovore.example.coast

import java.io.File

import com.google.common.base.Charsets
import com.google.common.io.Files
import com.monovore.coast
import com.monovore.coast.Flow

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
