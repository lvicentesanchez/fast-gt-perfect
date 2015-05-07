package com.mindcandy.data.model

import argonaut.{ EncodeJson, DecodeJson }

case class Tag(value: String) extends AnyVal

object Tag {
  implicit val decode: DecodeJson[Tag] = DecodeJson.of[String].map(Tag(_))
  implicit val encode: EncodeJson[Tag] = EncodeJson.of[String].contramap(_.value)
}
