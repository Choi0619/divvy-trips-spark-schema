// Name of the package
name := "module-04"

// Version of the package
version := "1.0"

// Version of Scala
scalaVersion := "2.12.10"

// Spark library dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.0",
  "org.apache.spark" %% "spark-sql" % "3.4.0"
)