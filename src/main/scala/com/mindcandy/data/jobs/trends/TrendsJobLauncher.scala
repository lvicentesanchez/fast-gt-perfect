package com.mindcandy.data.jobs.trends

import com.datastax.spark.connector.SelectableColumnRef
import com.mindcandy.data.Launcher
import com.mindcandy.data.jobs.FolderProducerBaseJob
import com.twitter.algebird.SpaceSaver
import com.typesafe.config.Config
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.joda.time.DateTime
import scala.concurrent.duration._

object TrendsJobLauncher extends TrendsJob with FolderProducerBaseJob with Launcher {
  override val Bucket: FiniteDuration = 5.minutes
  override val Capacity: Int = 400
  override val CF: String = "trends"
  override val Columns: Seq[SelectableColumnRef] = Seq("time", "tags")
  override val KS: String = "fast"

  override def run(config: Config, ssc: StreamingContext): Unit = {
    val events: DStream[String] = produce(config, ssc)
    val output: DStream[(DateTime, SpaceSaver[String])] = process(events)
    mergeAndStore(output)
  }
}
