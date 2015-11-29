resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "0.9.0.0",
  "com.lmax" % "disruptor" % "3.3.2",
  "ch.qos.logback" % "logback-classic" % "1.1.3"
)