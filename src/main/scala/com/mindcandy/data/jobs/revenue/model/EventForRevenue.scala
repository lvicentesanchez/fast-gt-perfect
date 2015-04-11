package com.mindcandy.data.jobs.revenue.model

import argonaut._, Argonaut._
import com.mindcandy.data.codecs.datetime.iso._
import com.mindcandy.data.model.{ Amount, TxID }
import org.joda.time.DateTime

case class EventForRevenue(time: DateTime, txid: TxID, amount: Amount)

object EventForRevenue {
  implicit val codec: CodecJson[EventForRevenue] = casecodec3(EventForRevenue.apply _, EventForRevenue.unapply _)("fired_ts", "tx_id", "amount")
}
