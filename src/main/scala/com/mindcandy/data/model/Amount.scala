package com.mindcandy.data.model

import argonaut.{ DecodeJson, EncodeJson }

case class Amount(value: Int) extends AnyVal

object Amount {
  implicit val decode: DecodeJson[Amount] = DecodeJson.of[Int].map(Amount(_))
  implicit val encode: EncodeJson[Amount] = EncodeJson.of[Int].contramap(_.value)
}