version := "0.1.0-SNAPSHOT"
name := "graphs"
scalaVersion := "2.13.8"

scalacOptions += "-target:jvm-11"

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % "3.2.0"),
  ("org.apache.spark" %% "spark-sql" % "3.2.0"),
  ("org.apache.spark" %% "spark-graphx" % "3.2.0"),
  "org.scalameta" %% "munit" % "0.7.29" % Test
)