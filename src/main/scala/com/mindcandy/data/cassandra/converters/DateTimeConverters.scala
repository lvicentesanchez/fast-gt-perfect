package com.mindcandy.data.cassandra.converters

import com.datastax.spark.connector.types._
import java.util.Date
import org.joda.time.DateTime
import scala.reflect.runtime.universe._

trait AnyToDateTimeConverter extends TypeConverter[DateTime] {
  def targetTypeTag: TypeTag[DateTime] = typeTag[DateTime]

  def convertPF: PartialFunction[Any, DateTime] = {
    case long: Long => new DateTime(long)
    case date: Date => new DateTime(date)
  }
}

object AnyToDateTimeConverter extends AnyToDateTimeConverter

trait DateTimeToLongConverter extends TypeConverter[Long] {
  def targetTypeTag: TypeTag[Long] = typeTag[Long]

  def convertPF: PartialFunction[Any, Long] = {
    case date: DateTime => date.getMillis
  }
}

object DateTimeToLongConverter extends DateTimeToLongConverter

trait DateTimeToDateConverter extends TypeConverter[Date] {
  def targetTypeTag: TypeTag[Date] = typeTag[Date]

  def convertPF: PartialFunction[Any, Date] = {
    case date: DateTime => date.toDate
  }
}

object DateTimeToDateConverter extends DateTimeToDateConverter
