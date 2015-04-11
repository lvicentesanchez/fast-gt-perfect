package com.mindcandy.data.codecs

import argonaut._, Argonaut._
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

trait DateTimeCodecs {
  object iso {
    val UTCFormat = ISODateTimeFormat.dateTime.withZoneUTC()

    implicit def DateTimeAsISOUTCEncodeJson: EncodeJson[DateTime] = EncodeJson(s => jString(s.toString(UTCFormat)))
    implicit def DateTimeAsISOUTCDecodeJson: DecodeJson[DateTime] = implicitly[DecodeJson[String]].map(UTCFormat.parseDateTime)
  }
}