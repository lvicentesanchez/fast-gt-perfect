package com.mindcandy.data

import com.mindcandy.data.config.SparkConfig
import com.typesafe.config.{ Config, ConfigFactory }
import net.ceedubs.ficus.Ficus._
import org.apache.spark.streaming.{ Seconds, StreamingContext }

trait Launcher {
  def run(config: Config, ssc: StreamingContext): Unit

  val appConfig: Config = ConfigFactory.load()
  val spkConfig: SparkConfig = appConfig.as[SparkConfig]("sparkConf")
  val streaming: StreamingContext = new StreamingContext(spkConfig.sparkConf, Seconds(spkConfig.batch.toSeconds))

  run(appConfig, streaming)
  Console.in.readLine()
  streaming.stop()
  streaming.awaitTermination()
}
