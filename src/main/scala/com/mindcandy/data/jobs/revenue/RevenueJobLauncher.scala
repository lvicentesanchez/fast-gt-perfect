package com.mindcandy.data.jobs.revenue

import com.datastax.spark.connector.SelectableColumnRef
import com.mindcandy.data.Launcher
import com.mindcandy.data.jobs.FolderProducerBaseJob
import com.mindcandy.data.model.{ Amount, TxID }
import com.twitter.algebird.{ BloomFilter, BloomFilterMonoid }
import com.typesafe.config.Config
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.joda.time.DateTime

import scala.concurrent.duration._

object RevenueJobLauncher extends RevenueJob with FolderProducerBaseJob with Launcher {
  override val Bucket: FiniteDuration = 5.minutes
  val Capacity: Int = 10000
  override val KS: String = "fast"
  override val ColumnFamily: String = "revenue"
  val FalsePositive: Double = 0.01
  override val Monoid: BloomFilterMonoid = BloomFilter(Capacity, FalsePositive)

  override def run(config: Config, ssc: StreamingContext): Unit = {
    val events: DStream[String] = produce(config, ssc)
    val output: DStream[(DateTime, Iterable[(TxID, Amount)])] = process(events)
    mergeAndStore(output)
  }
}
