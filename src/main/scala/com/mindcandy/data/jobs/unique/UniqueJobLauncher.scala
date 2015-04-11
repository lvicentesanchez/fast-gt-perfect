package com.mindcandy.data.jobs.unique

import com.mindcandy.data.Launcher
import com.mindcandy.data.jobs.FileProducerBaseJob
import com.twitter.algebird.{ HLL, HyperLogLogMonoid }
import com.typesafe.config.Config
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.joda.time.DateTime

object UniqueJobLauncher extends UniqueJob with FileProducerBaseJob with Launcher {
  val monoid: HyperLogLogMonoid = new HyperLogLogMonoid(12)

  def run(config: Config, ssc: StreamingContext): Unit = {
    val events: DStream[String] = produce(config, ssc)
    val output: DStream[(DateTime, HLL)] = process(events)
    updateAndStore(output)
  }
}
