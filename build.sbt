name := "Services-module"

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)

version := "0.1"

scalaVersion := "2.12.5"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % "2.12.4",
  "org.scala-lang" % "scala-compiler" % "2.12.4",
  "org.json4s" % "json4s-jackson_2.12" % "3.6.0-M2",
  "io.reactivex" %% "rxscala" % "0.26.5",
  "io.reactivex.rxjava2" % "rxjava" % "2.1.10",
  "com.github.pureconfig" %% "pureconfig" % "0.9.0",
  "org.scalamacros" % "paradise_2.12.4" % "2.1.1",
  "org.log4s" %% "log4s" % "1.6.0",
  "org.apache.xbean" % "xbean-finder" % "4.7"
)