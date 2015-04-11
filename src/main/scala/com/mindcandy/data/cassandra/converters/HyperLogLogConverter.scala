package com.mindcandy.data.cassandra.converters

import java.nio.ByteBuffer

import com.datastax.spark.connector.types._
import com.twitter.algebird.{ HLL, HyperLogLog }
import scala.reflect.runtime.universe._

trait AnyToHyperLogLogConverter extends TypeConverter[HLL] {
  def targetTypeTag: TypeTag[HLL] = typeTag[HLL]

  def convertPF: PartialFunction[Any, HLL] = {
    case bytes: Array[Byte] => HyperLogLog.fromBytes(bytes)
    case bytes: ByteBuffer => HyperLogLog.fromByteBuffer(bytes)
  }
}

object AnyToHyperLogLogConverter extends AnyToHyperLogLogConverter

trait HyperLogLogToArrayByteConverter extends TypeConverter[Array[Byte]] {
  def targetTypeTag: TypeTag[Array[Byte]] = typeTag[Array[Byte]]

  def convertPF: PartialFunction[Any, Array[Byte]] = {
    case counter: HLL => HyperLogLog.toBytes(counter)
  }
}

object HyperLogLogToArrayByteConverter extends HyperLogLogToArrayByteConverter
