// Comment to get more information during initialization
//
logLevel := Level.Warn

// Resolvers
//

resolvers += "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"
      
resolvers += "Softprops Maven" at "http://dl.bintray.com/content/softprops/maven"

// Dependency graph
//
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.7.4")

// Scalariform
//
addSbtPlugin("com.typesafe.sbt" % "sbt-scalariform" % "1.3.0")

// Update plugin
//
addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.1.7")
