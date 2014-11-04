def samzaDep(name: String) = "org.apache.samza" %% name % "0.7.0"

libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "18.0",
  "com.google.code.findbugs" % "jsr305" % "1.3.9" % "provided",
  "org.apache.samza" % "samza-api" % "0.7.0",
  samzaDep("samza-core"),
  samzaDep("samza-kv"),
  "ch.qos.logback" % "logback-classic" % "1.1.2"
)

// integration test dependencies
libraryDependencies ++= Seq(
  samzaDep("samza-kafka") % "it",
  "org.apache.kafka" %% "kafka" % "0.8.1.1" % "it" classifier "test"
    exclude("javax.jms", "jms") exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri"),
  "org.specs2" %% "specs2" % "2.4.2" % "it",
  "org.scalacheck" %% "scalacheck" % "1.11.5" % "it"
)

fork in run := true