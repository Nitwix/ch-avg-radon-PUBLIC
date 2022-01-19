name := "ch-avg-radon-concentration"

scalaVersion := "2.12.15"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.10"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % "test"

libraryDependencies += "org.tinylog" % "tinylog-impl" % "2.3.2"
libraryDependencies += "org.tinylog" %% "tinylog-api-scala" % "2.3.2"

libraryDependencies += "org.tinylog" % "tinylog-impl" % "2.3.2" % Test
libraryDependencies += "org.tinylog" %% "tinylog-api-scala" % "2.3.2"  % Test

libraryDependencies += "com.github.wookietreiber" %% "scala-chart" % "0.5.1" // for plotting the lognormal distribution
libraryDependencies += "org.jfree" % "jfreesvg" % "3.0" // for svg export