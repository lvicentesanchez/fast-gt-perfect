package com.mindcandy.data.model

import argonaut.{ DecodeJson, EncodeJson }

case class UserID(value: String) extends AnyVal

object UserID {
  implicit val decode: DecodeJson[UserID] = DecodeJson.of[String].map(UserID(_))
  implicit val encode: EncodeJson[UserID] = EncodeJson.of[String].contramap(_.value)
}
