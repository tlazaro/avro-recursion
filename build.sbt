import sbt.Keys.scalacOptions

lazy val sharedSettings: Seq[Setting[_]] = Seq[Setting[_]](
  scalaVersion := "2.13.5",
  organization := "com.belfrygames",
  version := "0.1",
  libraryDependencies ++= Seq(
    "org.typelevel"              %% "cats-core"     % "2.5.0",
    "org.typelevel"              %% "kittens"       % "2.2.1",
    "com.chuusai"                %% "shapeless"     % "2.3.3",
    "io.higherkindness"          %% "droste-core"   % "0.8.0",
    "io.higherkindness"          %% "droste-macros" % "0.8.0",
    "org.apache.avro"            % "avro"           % "1.9.2",
    "org.slf4j"                  % "slf4j-log4j12"  % "1.7.30",
    "com.beachape"               %% "enumeratum"    % "1.6.1",
    "com.github.pathikrit"       %% "better-files"  % "3.9.1",
    "io.circe"                   %% "circe-core"    % "0.14.0-M4",
    "io.circe"                   %% "circe-generic" % "0.14.0-M4",
    "io.circe"                   %% "circe-parser"  % "0.14.0-M4",
    "com.github.julien-truffaut" %% "monocle-core"  % "3.0.0-M4",
    "com.github.julien-truffaut" %% "monocle-macro" % "3.0.0-M4",
    "org.scalactic"              %% "scalactic"     % "3.2.7",
    "org.scalatest"              %% "scalatest"     % "3.2.7" % "test",
    "org.scalacheck"             %% "scalacheck"    % "1.15.3" % "test"
  ),
  javacOptions ++= Seq("-encoding", "UTF-8"),
  scalacOptions in Global += "-Ymacro-annotations",
  fork in Test := false,
  fork := true,
  addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.11.3" cross CrossVersion.full),
  scalacOptions ++= ScalacOptions.all
)

lazy val avro = project.in(file("avro")).settings(sharedSettings: _*)

lazy val root = project.in(file(".")).settings(sharedSettings: _*).aggregate(avro)
