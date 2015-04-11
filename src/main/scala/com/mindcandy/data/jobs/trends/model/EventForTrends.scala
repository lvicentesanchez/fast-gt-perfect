package com.mindcandy.data.jobs.trends.model

import argonaut._, Argonaut._
import com.mindcandy.data.codecs.datetime.iso._
import com.mindcandy.data.model.Tag
import org.joda.time.DateTime

case class EventForTrends(time: DateTime, tags: List[Tag])

object EventForTrends {
  implicit val codec: CodecJson[EventForTrends] = casecodec2(EventForTrends.apply _, EventForTrends.unapply _)("fired_ts", "tags")
}
