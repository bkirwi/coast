package com.monovore.coast.samza

import java.io.{File, FileOutputStream}
import java.util.Properties

import com.google.common.base.Charsets
import com.google.common.io.Files
import com.monovore.coast.core.Graph
import com.monovore.coast.viz.Dot
import joptsimple.{OptionParser, OptionSet}
import org.apache.samza.config.Config
import org.apache.samza.job.JobRunner
import org.apache.samza.util.{CommandLine, Logging}

import scala.collection.JavaConverters._

/**
 * A superclass that provides a basic CLI for basic Samza apps.
 * 
 * @param backend The Samza backend this app should use. (eg. `SimpleBackend`
 *                or `SafeBackend`)
 */
abstract class SamzaApp(backend: SamzaBackend) extends Logging {

  import SamzaApp._

  /**
   * The dataflow graph for this job.
   */
  def graph: Graph

  def main(args: Array[String]): Unit = {

    args.toList match {
      case "dot" :: rest => {

        val dotParser = new OptionParser()

        dotParser
          .accepts("to-file", "Print the graph to the specified file, instead of stdout.")
          .withRequiredArg()

        val opts = dotParser.parse(rest: _*)

        (Option(opts.valueOf("to-file")): @unchecked) match {
          case Some(fileName: String) => Files.asCharSink(new File(fileName), Charsets.UTF_8).write(Dot.describe(graph))
          case None => println(Dot.describe(graph))
        }
      }
      case "gen-config" :: rest => {

        val opts = samzaCmd.parser.parse(rest: _*)
        val configs = withBaseConfig(opts)

        val targetDirectory = (Option(opts.valueOf("target-directory")): @unchecked) match {
          case Some(target: String) => {
            val targetDir = new File(target)
            if (!targetDir.isDirectory) {
              Console.err.println(s"Path $target is not a directory!")
              sys.exit(1)
            }
            targetDir
          }
          case None => {
            System.err.println("No target directory for config!")
            samzaCmd.parser.printHelpOn(System.err)
            sys.exit(1)
          }
        }

        generateConfigFiles(targetDirectory, configs)
      }
      case "run" :: rest => {

        val opts = samzaCmd.parser.parse(rest: _*)
        val configs = withBaseConfig(opts)

        for ((name, config) <- configs) {
          info(s"Launching samza job: $name")
          new JobRunner(config).run()
        }
      }
      case "info" :: rest => {

        val opts = samzaCmd.parser.parse(rest: _*)
        val configs = withBaseConfig(opts)

        for ((name, config) <- configs) {
          print(jobInfo(name, config))
        }
      }
      case unknown => Console.err.println("Available commands: dot, run, info, gen-config")
    }
  }

  private[this] val samzaCmd = {

    val cmd = new CommandLine

    cmd.parser
      .accepts("job", "Executes the command on the Samza job with the given name. Repeat to specify multiple jobs, or omit to run on all jobs.")
      .withRequiredArg()
      .describedAs("job-name")

    cmd.parser
      .accepts("target-directory", "When generating config, write it to the specified directory.")
      .withRequiredArg()
      .describedAs("./target/directory")

    cmd
  }

  private[this] def withBaseConfig(options: OptionSet): Map[String, Config] = {

    val baseConfig = samzaCmd.loadConfig(options)

    val jobFilter =
      Option(options.valuesOf("job"))
        .map { _.asScala.collect { case job: String => job }.toSet }
        .filter { _.nonEmpty }
        .getOrElse { _: String => true }

    backend(baseConfig).configure(graph)
      .filter { case (name, _) => jobFilter(name) }
  }
}

object SamzaApp {

  val helpText =
    """Invalid arguments! Try one of the following commands:
      |
      |  dot
      |    Print out a representation of the job graph in GraphViz's 'dot' format.
      |
      |  gen-config <base config path> <output dir>
      |    Writes the generated config for the job to the given output directory.
      |
      |  run <base config path>
      |    Launches all of the Samza jobs.
      |
      |  info <base config path>
      |    Prints a summary of inputs, outputs, and internal state for every
      |    generated Samza job.
    """.stripMargin

  def jobInfo(name: String, config: Config): String = {

    // TODO: less manual way to get this information!
    val ChangelogKey = "stores\\.(.+)\\.changelog".r
    val MergeKey = "systems\\.([^\\.]+)\\.streams\\.(.+)\\.merge".r
    val CheckpointKey = "stores\\.(.+)\\.type".r
    val CoastStream = s"[^\\.]+\\.(.+)".r

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
  }

  def generateConfigFiles(directory: File, configs: Map[String, Config]): Unit = {

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
