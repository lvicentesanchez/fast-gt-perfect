package com.mindcandy.data.cassandra.converters

import com.datastax.spark.connector.types._
import com.mindcandy.data.model.Amount
import java.{ lang => jl }
import scala.reflect.runtime.universe._

trait AnyToAmountConverter extends TypeConverter[Amount] {
  def targetTypeTag: TypeTag[Amount] = typeTag[Amount]

  def convertPF: PartialFunction[Any, Amount] = {
    case int: Int => Amount(int)
    case int: jl.Integer => Amount(int)
  }
}

object AnyToAmountConverter extends AnyToAmountConverter

trait AmountToIntegerConverter extends TypeConverter[jl.Integer] {
  def targetTypeTag: TypeTag[jl.Integer] = typeTag[jl.Integer]

  def convertPF: PartialFunction[Any, jl.Integer] = {
    case amount: Amount => amount.value
  }
}

object AmountToIntegerConverter extends AmountToIntegerConverter
