
name := "coast"

scalaVersion := "2.10.4"

scalacOptions := Seq("-feature")

libraryDependencies ++= Seq(
  "com.chuusai" % "shapeless_2.10.4" % "2.0.0",
  "com.netflix.rxjava" % "rxjava-scala" % "0.20.3" exclude ("org.scala-lang", "scala-library"),
  "org.specs2" %% "specs2" % "2.4.2" % "test",
  "org.scalacheck" %% "scalacheck" % "1.11.5" % "test"
)
