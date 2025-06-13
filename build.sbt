ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.16"

libraryDependencies += "org.postgresql" % "postgresql"  % "42.7.6"
libraryDependencies += "com.beust" % "jcommander"  % "1.82"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.5"
libraryDependencies += "org.apache.spark" %% "spark-sql"  % "3.5.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.18" % Test

lazy val root = (project in file("."))
  .settings(
    name := "tracks"
  )
