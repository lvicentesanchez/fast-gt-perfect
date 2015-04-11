package com.mindcandy.data.jobs.unique

import argonaut._
import com.mindcandy.data.jobs.BaseJob
import com.mindcandy.data.jobs.unique.model.EventForUnique
import com.twitter.algebird.HyperLogLogMonoid
import org.apache.spark.streaming.dstream.DStream

trait UniqueJob extends BaseJob {
  def monoid: HyperLogLogMonoid
  def extract(input: DStream[String]): DStream[EventForUnique] =
    input.flatMap(Parse.decodeOption[EventForUnique](_))
}
