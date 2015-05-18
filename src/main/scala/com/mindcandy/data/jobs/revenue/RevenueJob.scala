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

trait RevenueJob { self: BaseJob =>
  def Bucket: FiniteDuration
  def KS: String
  def ColumnFamily: String
  // Needed TypeConverter to create an implicit RowReaderFactory
  //
  implicit val DateTimeConverter: TypeConverter[DateTime] = AnyToDateTimeConverter
  implicit val BloomFilterConverter: TypeConverter[BF] = AnyToBloomFilterConverter
  //
  override val Converters: Seq[TypeConverter[_]] = Seq(
    AmountToIntegerConverter,
    AnyToAmountConverter,
    DateTimeConverter,
    BloomFilterConverter,
    BloomFilterToArrayByteConverter,
    BloomFilterToByteBufferConverter,
    DateTimeToDateConverter,
    DateTimeToLongConverter
  )
  def Monoid: BloomFilterMonoid

  def extract(input: DStream[String]): DStream[EventForRevenue] =
    input.flatMap(Parse.decodeOption[EventForRevenue](_))

  def filterAndMerge(current: Iterable[(TxID, Amount)], previous: Option[(BF, Amount)]) = {
    val (bloomFilter, revenue): (BF, Amount) = previous.getOrElse((Monoid.zero, Amount(0)))
    val filtered: Iterable[(TxID, Amount)] = current.filter {
      case (TxID(txid), _) => !bloomFilter.contains(txid).isTrue
    }
    val updatedBloomFilter: BF = filtered.foldLeft(bloomFilter) {
      case (acc, (TxID(txid), _)) => acc + txid
    }
    val updatedRevenue: Amount = filtered.foldLeft(revenue) {
      case (Amount(acc), (_, Amount(value))) => Amount(acc + value)
    }
    (updatedBloomFilter, updatedRevenue)
  }

  def mergeAndStore(data: DStream[(DateTime, Iterable[(TxID, Amount)])]): Unit =
    data.foreachRDD { rdd =>
      val loaded: RDD[(DateTime, (BF, Amount))] =
        rdd.joinWithCassandraTable[(DateTime, BF, Amount)](KS, ColumnFamily)
          .select("time", "bf", "amount").map {
            case (_, (time, bf, amount)) => (time, (bf, amount))
          }
      val output: RDD[(DateTime, BF, Amount)] =
        rdd.leftOuterJoin(loaded).map {
          case (time, (current, previous)) =>
            val (bloomFilter, revenue): (BF, Amount) = filterAndMerge(current, previous)
            (time, bloomFilter, revenue)
        }
      output.saveToCassandra(KS, ColumnFamily, SomeColumns("time", "bf", "amount"))
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
