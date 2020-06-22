name := "spark-scalatest-helper"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"

javacOptions ++= Seq(
  "-source", "1.8",
  "-target", "1.8",
)

val scalatestVersion = "3.1.2"

lazy val commonDependencies = Seq(
  "org.scalatest" %% "scalatest" % scalatestVersion,
  "org.scalactic" %% "scalactic" % scalatestVersion
)

lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
)

fork in Test := true
//Compile / doc / scalacOptions := Seq("-groups", "-implicits")
autoAPIMappings := true
apiURL := Some(url("http://spark.apache.org/docs/2.4.0/api/scala/"))

libraryDependencies ++= commonDependencies ++ sparkDependencies
