package com.mindcandy.data.jobs

import com.datastax.spark.connector.types.TypeConverter
import com.typesafe.config.Config
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.joda.time.DateTime

import scala.concurrent.duration.FiniteDuration

trait BaseJob {
  def convert: Seq[TypeConverter[_]]
  def produce(config: Config, streaming: StreamingContext): DStream[String]
  def buckets[A, B](f: A => DateTime, g: A => B, bucket: FiniteDuration)(input: DStream[A]): DStream[(DateTime, B)] =
    input.map { data =>
      val time: DateTime = f(data)
      val rounded: DateTime = new DateTime((time.getMillis / bucket.toMillis) * bucket.toMillis)
      (rounded, g(data))
    }

  convert.foreach(TypeConverter.registerConverter)
}
