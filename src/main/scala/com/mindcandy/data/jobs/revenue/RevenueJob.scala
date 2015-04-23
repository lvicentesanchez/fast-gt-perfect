package com.mindcandy.data.jobs.revenue

import argonaut._
import com.datastax.spark.connector._
import com.datastax.spark.connector.types.TypeConverter
import com.mindcandy.data.cassandra.converters._
import com.mindcandy.data.jobs.BaseJob
import com.mindcandy.data.jobs.revenue.model.EventForRevenue
import com.mindcandy.data.model.{ Amount, TxID }
import com.twitter.algebird._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.joda.time.DateTime

import scala.concurrent.duration._

trait RevenueJob extends BaseJob {
  def Bucket: FiniteDuration
  def CF: String
  def Columns: Seq[SelectableColumnRef]
  val Converters: Seq[TypeConverter[_]] = Seq(
    AnyToDateTimeConverter,
    AnyToBloomFilterConverter(Cache),
    BloomFilterToArrayByteConverter(Cache),
    DateTimeToDateConverter,
    DateTimeToLongConverter
  )
  def KS: String
  def Monoid: BloomFilterMonoid

  def extract(input: DStream[String]): DStream[EventForRevenue] =
    input.flatMap(Parse.decodeOption[EventForRevenue](_))

  def filter(bloom: BF, current: Iterable[(TxID, Amount)]): Iterable[(TxID, Amount)] =
    current.filter {
      case (TxID(txid), _) => !bloom.contains(txid).isTrue
    }

  def mergeBF(bloom: BF, current: Iterable[(TxID, Amount)]): BF =
    current.foldLeft(bloom) {
      case (acc, (TxID(txid), _)) => acc + txid
    }

  def mergeAmount(amount: Int, current: Iterable[(TxID, Amount)]): Int =
    current.foldLeft(amount) {
      case (acc, (_, Amount(value))) => acc + value
    }

  def filterAndMerge(current: Iterable[(TxID, Amount)], previous: Option[(BF, Int)]): (BF, Int) = {
    val (bf, amount): (BF, Int) = previous.getOrElse((Monoid.zero, 0))
    val filtered: Iterable[(TxID, Amount)] = filter(bf, current)
    val updatedBF: BF = mergeBF(bf, filtered)
    val updatedRV: Int = mergeAmount(amount, filtered)
    (updatedBF, updatedRV)
  }

  def mergeAndStore(data: DStream[(DateTime, Iterable[(TxID, Amount)])]): Unit =
    data.foreachRDD { rdd =>
      rdd.cache()
      val loaded: RDD[(DateTime, (BF, Int))] =
        rdd.joinWithCassandraTable[(DateTime, BF, Int)](KS, CF).select(Columns: _*).map {
          case (_, (time, bf, amount)) => (time, (bf, amount))
        }
      val output: RDD[(DateTime, BF, Int)] =
        rdd.leftOuterJoin(loaded).map {
          case (time, (current, previous)) =>
            val (updatedBF, updatedRV): (BF, Int) = filterAndMerge(current, previous)
            (time, updatedBF, updatedRV)
        }
      output.saveAsCassandraTable(KS, CF, SomeColumns(Columns: _*))
      rdd.unpersist(blocking = false)
    }

  def process(data: DStream[String]): DStream[(DateTime, Iterable[(TxID, Amount)])] = {
    val extracted: DStream[EventForRevenue] = extract(data)
    val withBuckets: DStream[(DateTime, (TxID, Amount))] =
      buckets[EventForRevenue, (TxID, Amount)](_.time, tupled, Bucket)(extracted)
    val grouped: DStream[(DateTime, Iterable[(TxID, Amount)])] =
      withBuckets.groupByKey()
    grouped
  }

  def tupled(event: EventForRevenue): (TxID, Amount) =
    (event.txid, event.amount)
}
