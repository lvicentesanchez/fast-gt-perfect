package com.mindcandy.data.jobs

import com.typesafe.config.Config
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

trait KafkaProducerBaseJob extends BaseJob {
  def produce(config: Config, streaming: StreamingContext): DStream[String] = ???
}
