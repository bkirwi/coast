import sbt._
import Keys._

object CoastBuild extends Build {

  import bintray.Plugin._

  lazy val coast = Project(
    id = "coast",
    base = file(".")
  ) aggregate (
    core, samza
  ) settings(

    // global project settings
    scalaVersion  in ThisBuild := "2.10.4",
    scalacOptions in ThisBuild := Seq("-feature", "-language:higherKinds"),

    organization in ThisBuild := "com.monovore",
    version in ThisBuild := "0.3.0-SNAPSHOT",

    licenses in ThisBuild += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html")),

    // make it possible to cancel forked processes with ctrl-c
    cancelable in Global := true,

    // No tests in aggregate project
    test := (),
    publish := (),

    libraryDependencies in ThisBuild ++= Seq(
      "org.specs2" %% "specs2" % "2.4.15" % "test",
      "org.scalacheck" %% "scalacheck" % "1.12.1" % "test"
    ),

    publishMavenStyle := false
  )

  lazy val core = Project(
    id = "coast-core",
    base = file("core")
  ) settings (
    bintrayPublishSettings: _*
  )

  lazy val samza = Project(
    id = "coast-samza",
    base = file("samza")
  ) dependsOn (
    core
  ) configs(
    IntegrationTest
  ) settings (
    bintrayPublishSettings ++ Defaults.itSettings : _*
  )
}
