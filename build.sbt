import sbt.Keys._

// sbt-dependecy-graph
import net.virtualvoid.sbt.graph.Plugin._

// sbt-scalariform
import com.typesafe.sbt.SbtScalariform._
import scalariform.formatter.preferences._

// Resolvers
resolvers ++= Seq(
  "sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
)

// Dependencies

val testDependencies = Seq(
  "org.specs2"             %% "specs2"                    % "2.4.16" % "test,it"
)

val rootDependencies = Seq(
  "joda-time"              %  "joda-time"                 % "2.7",
  "log4j"                  %  "apache-log4j-extras"       % "1.2.17",
  "com.datastax.spark"     %% "spark-cassandra-connector" % "1.2.0-rc3" exclude("org.apache.spark", "spark-core_2.10") exclude("org.apache.spark", "spark-streaming_2.10"),
  "com.twitter"            %% "algebird-core"             % "0.9.0",
  "io.argonaut"            %% "argonaut"                  % "6.1-M6",
  "net.ceedubs"            %% "ficus"                     % "1.0.1",
  "org.apache.spark"       %% "spark-core"                % "1.2.1" % "provided",
  "org.apache.spark"       %% "spark-streaming"           % "1.2.1" % "provided",
  "org.apache.spark"       %% "spark-streaming-kafka"     % "1.2.1" exclude("org.apache.spark", "spark-core_2.10") exclude("org.apache.spark", "spark-streaming_2.10"),
  "org.joda"               %  "joda-convert"              % "1.7"
)

val dependencies =
  rootDependencies ++
  testDependencies

// Settings
//
val forkedJvmOption = Seq(
  "-server",
  "-Dfile.encoding=UTF8",
  "-Duser.timezone=GMT",
  "-Xss1m",
  "-Xms4096m",
  "-Xmx4096m",
  "-XX:+CMSClassUnloadingEnabled",
  "-XX:+DoEscapeAnalysis",
  "-XX:+UseConcMarkSweepGC",
  "-XX:+UseParNewGC",
  "-XX:+UseCodeCacheFlushing",
  "-XX:+UseCompressedOops",
  "-XX:MaxTenuringThreshold=15",
  "-Xmn1280m",
  "-XX:SurvivorRatio=4"
)

val compileSettings = Seq(
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding", "UTF-8",
    "-target:jvm-1.7",
    "-feature",
    "-language:_",
    "-unchecked",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard",
    "-Xfuture"
  )
)

val buildSettings = Seq(
  name := "fastgtperfect",
  organization := "com.mindcandy.data",
  scalaVersion := "2.10.5",
  scalaBinaryVersion := "2.10"
)

val formatting =
  FormattingPreferences()
    .setPreference(AlignParameters, true)
    .setPreference(AlignSingleLineCaseStatements, false)
    .setPreference(AlignSingleLineCaseStatements.MaxArrowIndent, 40)
    .setPreference(CompactControlReadability, false)
    .setPreference(CompactStringConcatenation, false)
    .setPreference(DoubleIndentClassDeclaration, true)
    .setPreference(FormatXml, true)
    .setPreference(IndentLocalDefs, false)
    .setPreference(IndentPackageBlocks, true)
    .setPreference(IndentSpaces, 2)
    .setPreference(IndentWithTabs, false)
    .setPreference(MultilineScaladocCommentsStartOnFirstLine, false)
    .setPreference(PlaceScaladocAsterisksBeneathSecondAsterisk, false)
    .setPreference(PreserveSpaceBeforeArguments, false)
    .setPreference(PreserveDanglingCloseParenthesis, true)
    .setPreference(RewriteArrowSymbols, false)
    .setPreference(SpaceBeforeColon, false)
    .setPreference(SpaceInsideBrackets, false)
    .setPreference(SpaceInsideParentheses, false)
    .setPreference(SpacesWithinPatternBinders, true)


val pluginsSettings =
  buildSettings ++
  graphSettings ++
  scalariformSettingsWithIt

val settings = Seq(
  libraryDependencies ++= dependencies,
  fork in run := true,
  fork in Test := true,
  fork in testOnly := true,
  connectInput in run := true,
  javaOptions in run ++= forkedJvmOption,
  javaOptions in IntegrationTest ++= forkedJvmOption,
  javaOptions in Test ++= forkedJvmOption,
  // formatting
  //
  ScalariformKeys.preferences := formatting,
  // trick that makes provided dependencies work when running
  //
  run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)),
  // it
  unmanagedSourceDirectories in IntegrationTest <++= { baseDirectory { base => { Seq( base / "src/test/scala" )}}}
)

lazy val main =
  project
    .in(file("."))
    .configs(IntegrationTest)
    .settings(
      pluginsSettings ++ compileSettings ++ Defaults.itSettings ++ settings:_*
    )
