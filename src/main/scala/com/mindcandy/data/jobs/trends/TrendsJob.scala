package com.mindcandy.data.jobs.trends

import argonaut._
import com.datastax.spark.connector._
import com.datastax.spark.connector.types.TypeConverter
import com.mindcandy.data.cassandra.converters._
import com.mindcandy.data.cassandra.reader._
import com.mindcandy.data.jobs.BaseJob
import com.mindcandy.data.jobs.trends.model.EventForTrends
import com.mindcandy.data.model.Tag
import com.twitter.algebird.SpaceSaver
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.joda.time.DateTime
import scala.concurrent.duration._

trait TrendsJob extends BaseJob {
  def Bucket: FiniteDuration
  def Capacity: Int
  def CF: String
  def Columns: Seq[SelectableColumnRef]
  val Converters: Seq[TypeConverter[_]] = Seq(
    AnyToDateTimeConverter,
    AnyToSpaceSaverConverter,
    DateTimeToDateConverter,
    DateTimeToLongConverter,
    new SpaceSaverToArrayByteConverter[String]()
  )
  def KS: String

  def extract(input: DStream[String]): DStream[EventForTrends] =
    input.flatMap(Parse.decodeOption[EventForTrends](_))

  def mergeAndStore(data: DStream[(DateTime, SpaceSaver[String])]): Unit =
    data.foreachRDD { rdd =>
      rdd.cache()
      val loaded: RDD[(DateTime, SpaceSaver[String])] =
        rdd.joinWithCassandraTable[(DateTime, SpaceSaver[String])](KS, CF).select(Columns: _*).map {
          case (_, (time, previous)) => (time, previous)
        }
      val output: RDD[(DateTime, SpaceSaver[String])] =
        rdd.leftOuterJoin(loaded).map {
          case (time, (current, previous)) => (time, previous.fold(current)(_ ++ current))
        }
      output.saveAsCassandraTable(KS, CF, SomeColumns(Columns: _*))
      rdd.unpersist(blocking = false)
    }

  def process(data: DStream[String]): DStream[(DateTime, SpaceSaver[String])] = {
    val extracted: DStream[EventForTrends] = extract(data)
    val withBuckets: DStream[(DateTime, List[Tag])] =
      buckets[EventForTrends, List[Tag]](_.time, _.tags, Bucket)(extracted)
    val keyValue: DStream[(DateTime, SpaceSaver[String])] =
      withBuckets.flatMap { case (time, tags) => tags.map(tag => (time, SpaceSaver(Capacity, tag.value))) }
    val reduced: DStream[(DateTime, SpaceSaver[String])] =
      keyValue.reduceByKey(_ ++ _)
    reduced
  }
}
