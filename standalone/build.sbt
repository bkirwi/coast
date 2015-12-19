resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "0.9.0.0",
  "com.lmax" % "disruptor" % "3.3.2",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "io.reactivex" %% "rxscala" % "0.25.0",
  "com.typesafe.akka" %% "akka-stream-experimental" % "2.0-M2"
)