package com.mindcandy.data.cassandra.reader

import com.datastax.driver.core.{ ProtocolVersion, Row }
import com.datastax.spark.connector.rdd.reader.{ RowReader, ThisRowReaderAsFactory }
import com.twitter.algebird.{ HLL, HyperLogLog }
import org.joda.time.DateTime

trait UniqueRowReader extends RowReader[(DateTime, HLL)] with ThisRowReaderAsFactory[(DateTime, HLL)] {
  override def read(row: Row, columnNames: Array[String])(implicit protocolVersion: ProtocolVersion): (DateTime, HLL) =
    (new DateTime(row.getDate("time")), HyperLogLog.fromByteBuffer(row.getBytes("counter")))

  override def requiredColumns: Option[Int] = columnNames.map(_.size)

  override def columnNames: Option[Seq[String]] = Option(Seq("time", "counter"))

  override def targetClass: Class[(DateTime, HLL)] = classOf[(DateTime, HLL)]
}
