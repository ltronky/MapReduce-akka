name := """TesiArraySortScala"""

val akkaVersion = "2.3.6"
val sprayV = "1.3.2"

fork in run := true

version := "0.1"

scalaVersion := "2.11.2"

scalacOptions in Compile ++= Seq("-encoding", "UTF-8", "-target:jvm-1.7", "-deprecation", "-feature", "-unchecked", "-Xlog-reflective-calls", "-Xlint")

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
  "com.typesafe.akka" %% "akka-contrib" % akkaVersion,
  "io.spray" % "spray-can_2.11" % sprayV,
  "io.spray" % "spray-routing_2.11" % sprayV,
  "io.spray" % "spray-httpx_2.11" % sprayV,
  "org.fusesource" % "sigar" % "1.6.4")

javaOptions in run ++= Seq(
  "-Djava.library.path=./sigar",
  "-Xms128m", "-Xmx1024m",
  "-Dcom.sun.management.jmxremote.port=9999",
  "-Dcom.sun.management.jmxremote.authenticate=false",
  "-Dcom.sun.management.jmxremote.ssl=false",
  "-XX:+UnlockCommercialFeatures",
  "-XX:+FlightRecorder")
