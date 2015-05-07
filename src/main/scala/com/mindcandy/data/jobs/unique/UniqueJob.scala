package com.mindcandy.data.jobs.unique

import argonaut._
import com.datastax.spark.connector._
import com.datastax.spark.connector.types.TypeConverter
import com.mindcandy.data.cassandra.converters._
import com.mindcandy.data.jobs.BaseJob
import com.mindcandy.data.jobs.unique.model.EventForUnique
import com.mindcandy.data.model.UserID
import com.twitter.algebird.{ HLL, HyperLogLogMonoid }
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.joda.time.DateTime
import scala.concurrent.duration._

trait UniqueJob { self: BaseJob =>
  def Bucket: FiniteDuration
  def CF: String
  def Columns: Seq[SelectableColumnRef]
  // Needed TypeConverter to create an implicit RowReaderFactory
  //
  implicit val DateTimeConverter: TypeConverter[DateTime] = AnyToDateTimeConverter
  implicit val HyperLogLogConverter: TypeConverter[HLL] = AnyToHyperLogLogConverter
  //
  override val Converters: Seq[TypeConverter[_]] = Seq(
    DateTimeConverter,
    HyperLogLogConverter,
    DateTimeToDateConverter,
    DateTimeToLongConverter,
    HyperLogLogToArrayByteConverter,
    HyperLogLogToByteBufferConverter
  )
  def KS: String
  def Monoid: HyperLogLogMonoid

  def extract(input: DStream[String]): DStream[EventForUnique] =
    input.flatMap(Parse.decodeOption[EventForUnique](_))

  def mergeAndStore(data: DStream[(DateTime, HLL)]): Unit =
    data.foreachRDD { rdd =>
      rdd.cache()
      val loaded: RDD[(DateTime, HLL)] =
        rdd.joinWithCassandraTable[(DateTime, HLL)](KS, CF).select(Columns: _*).map {
          case (_, (time, previous)) => (time, previous)
        }
      val output: RDD[(DateTime, HLL)] =
        rdd.leftOuterJoin(loaded).map {
          case (time, (current, previous)) => (time, previous.fold(current)(_ + current))
        }
      output.saveToCassandra(KS, CF, SomeColumns(Columns: _*))

      rdd.unpersist(blocking = false)
    }

  def process(data: DStream[String]): DStream[(DateTime, HLL)] = {
    val extracted: DStream[EventForUnique] = extract(data)
    val withBuckets: DStream[(DateTime, UserID)] =
      buckets[EventForUnique, UserID](_.time, _.userID, Bucket)(extracted)
    val reduced: DStream[(DateTime, HLL)] =
      withBuckets.groupByKey().mapValues(Monoid.batchCreate(_)(_.value.getBytes))
    reduced
  }
}
