package com.mindcandy.data.model

import argonaut.{ DecodeJson, EncodeJson }

case class TxID(value: String) extends AnyVal

object TxID {
  implicit val decode: DecodeJson[TxID] = DecodeJson.of[String].map(TxID(_))
  implicit val encode: EncodeJson[TxID] = EncodeJson.of[String].contramap(_.value)
}
