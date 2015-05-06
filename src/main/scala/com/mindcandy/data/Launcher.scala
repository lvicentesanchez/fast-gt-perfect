package com.mindcandy.data

import com.mindcandy.data.config.SparkConfig
import com.typesafe.config.{ Config, ConfigFactory }
import net.ceedubs.ficus.Ficus._
import org.apache.spark.streaming.{ Seconds, StreamingContext }

trait Launcher {
  def TimeOut: Long = 10000

  def run(config: Config, ssc: StreamingContext): Unit

  def main(args: Array[String]): Unit = {

    if (args.length == 1) {
      val appConfig: Config = ConfigFactory.parseResources(args(0))
      val spkConfig: SparkConfig = appConfig.as[SparkConfig]("config")
      val streaming: StreamingContext = new StreamingContext(spkConfig.sparkConf, Seconds(spkConfig.batch.toSeconds))

      run(appConfig, streaming)
      Console.in.readLine()
      streaming.stop()
      streaming.awaitTermination(TimeOut)
    }
  }
}
