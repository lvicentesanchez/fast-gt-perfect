package com.mindcandy.data.config

import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader
import org.apache.spark.SparkConf

import scala.concurrent.duration.FiniteDuration

case class SparkConfig(batch: FiniteDuration, sparkConf: SparkConf)

object SparkConfig {
  implicit val reader: ValueReader[SparkConfig] = ValueReader.relative(
    config =>
      SparkConfig(
        config.as[FiniteDuration]("batch"),
        new SparkConf().setAll(config.as[Map[String, String]]("sparkConf"))
      )
  )
}
