ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.13"

lazy val root = (project in file("."))
  .settings(
    name := "SparkETLBVH"
  )
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.2",
  "org.apache.spark" %% "spark-sql" % "3.2.0",
  "org.postgresql" % "postgresql" % "42.2.24",
  "com.typesafe" % "config" % "1.4.1",
  "com.oracle.database.jdbc" % "ojdbc8" % "21.9.0.0",
  "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4"
)