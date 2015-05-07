package com.mindcandy.data.jobs

import com.mindcandy.data.config.FolderProducerConfig
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

trait FolderProducerBaseJob extends BaseJob {
  override def produce(config: Config, streaming: StreamingContext): DStream[String] =
    streaming.textFileStream(
      config.as[FolderProducerConfig]("producer").folder
    )
}
