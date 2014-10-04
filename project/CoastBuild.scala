import sbt._
import Keys._

object CoastBuild extends Build {
  
  lazy val coast = Project(
    id = "coast",
    base = file(".")
  ) aggregate (
    core
  ) settings(

    // global project settings
    scalaVersion  in ThisBuild := "2.10.4",
    scalacOptions in ThisBuild := Seq("-feature"),
    resolvers in ThisBuild += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",

    // make it possible to cancel forked processes with ctrl-c
    cancelable in Global := true,

    // No tests in aggregate project
    test := ()
  )

  lazy val core = Project(
    id = "coast-core",
    base = file("core")
  )
}
