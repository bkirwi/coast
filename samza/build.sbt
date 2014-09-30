def samzaDep(name: String) = "org.apache.samza" %% name % "0.7.0"

libraryDependencies ++= Seq(
  "com.google.code.findbugs" % "jsr305" % "1.3.9" % "provided",
  "org.apache.samza" %% "samza-core" % "0.7.0",
  "org.apache.samza" % "samza-api" % "0.7.0",
  samzaDep("samza-kafka"),
  "ch.qos.logback" % "logback-classic" % "1.1.2"
)

fork in run := true