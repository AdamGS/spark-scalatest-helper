name := "spark-scalatest-helper"

version := "0.1"

scalaVersion := "2.11.12"

val scalatestVersion = "3.1.2"
val sparkVersion = "2.4.0"

lazy val commonDependencies = Seq(
  "org.scalatest" %% "scalatest" % scalatestVersion,
  "org.scalactic" %% "scalactic" % scalatestVersion
)

lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
)

libraryDependencies ++= commonDependencies ++ sparkDependencies

fork in Test := true
javacOptions ++= Seq(
  "-source", "1.8",
  "-target", "1.8",
)