import sbt._
import Keys._

object CoastBuild extends Build {
  
  lazy val coast = Project(
    id = "coast",
    base = file(".")
  ) aggregate (
    core, samza
  ) settings(

    // global project settings
    scalaVersion  in ThisBuild := "2.10.4",
    scalacOptions in ThisBuild := Seq("-feature"),

    organization in ThisBuild := "com.monovore.coast",

    // make it possible to cancel forked processes with ctrl-c
    cancelable in Global := true,

    // No tests in aggregate project
    test := (),

    libraryDependencies in ThisBuild ++= Seq(
      "org.specs2" %% "specs2" % "2.4.11" % "test",
      "org.scalacheck" %% "scalacheck" % "1.11.5" % "test"
    ),

    publishTo in ThisBuild := Some(
      Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository"))
    )
  )

  lazy val core = Project(
    id = "coast-core",
    base = file("core")
  )

  lazy val samza = Project(
    id = "coast-samza",
    base = file("samza")
  ) dependsOn (
    core
  ) configs( IntegrationTest ) settings ( Defaults.itSettings : _*)
}
