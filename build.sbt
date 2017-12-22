name := "simple-stats"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "com.azavea" %% "vectorpipe" % "0.2.2"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
