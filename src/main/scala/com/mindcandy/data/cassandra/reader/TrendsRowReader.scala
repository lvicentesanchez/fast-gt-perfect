package com.mindcandy.data.cassandra.reader

import com.datastax.driver.core.{ ProtocolVersion, Row }
import com.datastax.spark.connector.rdd.reader.{ RowReader, ThisRowReaderAsFactory }
import com.twitter.algebird.SpaceSaver
import com.twitter.algebird.extensions._

import org.joda.time.DateTime

trait TrendsRowReader extends RowReader[(DateTime, SpaceSaver[String])] with ThisRowReaderAsFactory[(DateTime, SpaceSaver[String])] {
  override def read(row: Row, columnNames: Array[String])(implicit protocolVersion: ProtocolVersion): (DateTime, SpaceSaver[String]) =
    (new DateTime(row.getDate("time")), SpaceSaver.fromByteBuffer(row.getBytes("tags")))

  override def requiredColumns: Option[Int] = columnNames.map(_.size)

  override def columnNames: Option[Seq[String]] = Option(Seq("time", "trends"))

  override def targetClass: Class[(DateTime, SpaceSaver[String])] = classOf[(DateTime, SpaceSaver[String])]
}
