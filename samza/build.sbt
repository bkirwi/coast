def samzaDep(name: String) = "org.apache.samza" %% name % "0.9.0-SNAPSHOT"

resolvers += "Local Maven Repository" at s"file://${Path.userHome.absolutePath}/.m2/repository"

libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "18.0",
  "com.google.code.findbugs" % "jsr305" % "1.3.9" % "provided",
  samzaDep("samza-core"),
  samzaDep("samza-kv"),
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  // TODO: integration tests only
  samzaDep("samza-kv-inmemory") exclude ("com.google.guava", "guava"),
  samzaDep("samza-kafka") exclude("org.slf4j", "slf4j-log4j12") exclude("org.apache.zookeeper", "zookeeper"),
  "org.slf4j" % "log4j-over-slf4j" % "1.7.6"
)

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "0.8.2.1" classifier "test", // TODO: scope under IntegrationTest
  "org.specs2" %% "specs2" % "2.4.15" % "it",
  "org.scalacheck" %% "scalacheck" % "1.12.1" % "it"
)

fork in run := true

fork in IntegrationTest := true

parallelExecution in IntegrationTest := false

baseDirectory in (IntegrationTest, test) := { baseDirectory.value / "target" }