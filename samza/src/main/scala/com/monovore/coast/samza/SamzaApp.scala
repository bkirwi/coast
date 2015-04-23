package com.monovore.coast.samza

import java.io.{File, FileOutputStream}
import java.net.URI
import java.util.Properties

import com.google.common.base.Charsets
import com.google.common.io.Files
import com.monovore.coast.core.Graph
import com.monovore.coast.viz.Dot
import org.apache.samza.config.Config
import org.apache.samza.config.factories.PropertiesConfigFactory
import org.apache.samza.job.JobRunner
import org.apache.samza.util.Logging

import scala.collection.JavaConverters._

/**
 * A superclass that provides a basic CLI for basic Samza apps.
 * 
 * @param backend The Samza backend this app should use. (eg. `SimpleBackend`
 *                or `SafeBackend`)
 */
abstract class SamzaApp(backend: SamzaBackend) extends Logging {

  val helpText =
    """
      |Try one of the following commands:
      |
      |  print-dot
      |    Prints out a representation of the job graph in GraphViz's 'dot' format.
      |
      |  save-dot <filename>
      |    Writes a representation of the job graph in GraphViz's 'dot' format to the specified file.
      |
      |  gen-config <base config path> <output dir>
      |    Writes the generated config for the job to the given output directory.
      |
      |  run <base config path>
      |    Launches all of the Samza jobs.
      |
      |  run-only <base config path> [<job names>]
      |    Launches only the specified Samza job(s).
      |
      |  info <base config path>
      |    Prints a summary of inputs, outputs, and internal state.
    """.stripMargin

  /**
   * The dataflow graph for this job.
   */
  def graph: Graph

  def main(args: Array[String]): Unit = {

    args.toList match {
      case "print-dot" :: Nil => {
        println(Dot.describe(graph))
      }
      case "save-dot" :: fileName :: Nil => {
        Files.asCharSink(new File(fileName), Charsets.UTF_8).write(Dot.describe(graph))
      }
      case "gen-config" :: basePath :: targetPath :: Nil => {
        val configs = withBaseConfig(basePath)
        val targetDirectory = new File(targetPath)
        generateConfigFiles(targetDirectory, configs)
      }
      case "run" :: basePath :: Nil => {
        val configs = withBaseConfig(basePath)
        for ((name, config) <- configs) {
          info(s"Launching samza job: $name")
          new JobRunner(config).run()
        }
      }
      case "run-only" :: basePath :: jobs => {
        val configs = withBaseConfig(basePath).filterKeys(jobs.contains)
        for ((name, config) <- configs) {
          info(s"Launching samza job: $name")
          new JobRunner(config).run()
        }
      }
      case "info" :: basePath :: Nil => {

        // TODO: less manual way to get this information!

        val ChangelogKey = "stores\\.(.+)\\.changelog".r
        val MergeKey = "systems\\.([^\\.]+)\\.streams\\.(.+)\\.merge".r
        val CheckpointKey = "stores\\.(.+)\\.type".r

        val configs = withBaseConfig(basePath)

        val CoastStream = s"[^\\.]+\\.(.+)".r

        for ((name, config) <- configs) {

          val pairs = config.asScala.toSeq.sortBy { _._1 }

          val mergeStream = pairs
            .collectFirst { case (MergeKey(_, stream), _) => stream }

          val checkpointStream = pairs
            .collectFirst { case (CheckpointKey(store), "checkpoint") =>
              pairs.collectFirst { case (ChangelogKey(`store`), CoastStream(value)) => value }
            }
            .flatten

          val changelogs = pairs
            .collect {
              case (ChangelogKey(store), CoastStream(value)) => store -> value
            }
            .filterNot { case (_, stream) => checkpointStream.exists { _ == stream } }

          val inputs = config.get("task.inputs", "").split(",")
            .filter { _.nonEmpty }
            .collect { case CoastStream(name) => name }
            .filterNot { stream => mergeStream.exists { _ == stream } }

          println(
            s"""
               |$name:
               |
               |  input streams:
               |    ${inputs.mkString("\n    ")}
               |
               |  output stream:
               |    $name
               |
               |  merge stream:
               |    ${mergeStream.getOrElse("<none>")}
               |
               |  checkpoint stream:
               |    ${checkpointStream.getOrElse("<none>")}
               |
               |  stores:
               |    ${changelogs.map { case (store, changelog) => s"$store:\n      changelog: $changelog" }.mkString("\n    ")}
             """.stripMargin
          )
        }
      }
      case "help" :: Nil => println(helpText)
      case Nil => println("\nNo arguments provided!"); println(helpText)
      case unknown => println("\nUnrecognized arguments: " + unknown.mkString(" ")); println(helpText)
    }
  }

  private[this] def withBaseConfig(basePath: String): Map[String, Config] = {
    val baseConfigURI = new URI(basePath)
    val configFactory = new PropertiesConfigFactory
    val baseConfig = configFactory.getConfig(baseConfigURI)

    backend(baseConfig).configure(graph)
  }

  private[this] def generateConfigFiles(directory: File, configs: Map[String, Config]): Unit = {

    configs.foreach { case (name, config) =>

      val properties = new Properties()
      val propertiesFile = new File(directory, s"$name.properties")

      config.asScala.foreach { case (k, v) => properties.setProperty(k, v) }

      val fos = new FileOutputStream(propertiesFile)
      properties.store(fos, null)
      fos.close()
    }
  }
}
