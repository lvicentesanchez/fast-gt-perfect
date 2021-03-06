package com.mindcandy.data.jobs.unique

import com.datastax.spark.connector.SelectableColumnRef
import com.mindcandy.data.Launcher
import com.mindcandy.data.jobs.FolderProducerBaseJob
import com.twitter.algebird.{ HLL, HyperLogLogMonoid }
import com.typesafe.config.Config
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.joda.time.DateTime
import scala.concurrent.duration._

object UniqueJobLauncher extends UniqueJob with FolderProducerBaseJob with Launcher {
  override val Bucket: FiniteDuration = 5.minutes
  override val KS: String = "fast"
  override val ColumnFamily: String = "unique"
  override val Monoid: HyperLogLogMonoid = new HyperLogLogMonoid(12)

  override def run(config: Config, ssc: StreamingContext): Unit = {
    val events: DStream[String] = produce(config, ssc)
    val output: DStream[(DateTime, HLL)] = process(events)
    mergeAndStore(output)
  }
}
