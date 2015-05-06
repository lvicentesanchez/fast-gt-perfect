package com.mindcandy.data.config

import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader

case class FolderProducerConfig(folder: String)

object FolderProducerConfig {
  implicit val reader: ValueReader[FolderProducerConfig] = ValueReader.relative(
    config =>
      FolderProducerConfig(
        config.as[String]("folder")
      )
  )
}
