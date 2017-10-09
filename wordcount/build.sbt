//Name of your project
name := "WordCount"

//Version of your project
version := "1.0"

//Scala version required for the project
scalaVersion := "2.10.6"

//Add library dependencies here
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.36"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"
