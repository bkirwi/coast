def samzaDep(name: String) = "org.apache.samza" %% name % "0.8.0-SNAPSHOT"

resolvers += "Local Maven Repository" at s"file://${Path.userHome.absolutePath}/.m2/repository"

libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "18.0",
  "com.google.code.findbugs" % "jsr305" % "1.3.9" % "provided",
  samzaDep("samza-core"),
  samzaDep("samza-kv"),
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  // TODO: integration tests only
  samzaDep("samza-kv-inmemory"),
  samzaDep("samza-kafka"),
  "org.apache.kafka" %% "kafka" % "0.8.1.1" classifier "test" exclude("javax.jms", "jms") exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri"),
  "org.specs2" %% "specs2" % "2.4.7" % "it",
  "org.scalacheck" %% "scalacheck" % "1.11.5" % "it"
)

fork in run := true

fork in test in IntegrationTest := true

baseDirectory in (IntegrationTest, test) := { baseDirectory.value / "target" }