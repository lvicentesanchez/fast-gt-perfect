package com.mindcandy.data.cassandra.converters

import com.datastax.spark.connector.types._
import com.mindcandy.data.model.Amount
import scala.reflect.runtime.universe._

trait AnyToAmountConverter extends TypeConverter[Amount] {
  def targetTypeTag: TypeTag[Amount] = typeTag[Amount]

  def convertPF: PartialFunction[Any, Amount] = {
    case int: Int => Amount(int)
  }
}

object AnyToAmountConverter extends AnyToAmountConverter

trait AmountToIntConverter extends TypeConverter[Int] {
  def targetTypeTag: TypeTag[Int] = typeTag[Int]

  def convertPF: PartialFunction[Any, Int] = {
    case amount: Amount => amount.value
  }
}

object AmountToIntConverter extends AmountToIntConverter
