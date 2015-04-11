package com.mindcandy.data.jobs.unique.model

import argonaut.Argonaut._
import argonaut._
import com.mindcandy.data.codecs.datetime.iso._
import com.mindcandy.data.model.UserID
import org.joda.time.DateTime

case class EventForUnique(time: DateTime, userID: UserID)

object EventForUnique {
  implicit val codec: CodecJson[EventForUnique] = casecodec2(EventForUnique.apply _, EventForUnique.unapply _)("fired_ts", "user_id")
}
