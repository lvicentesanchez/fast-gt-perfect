package com.mindcandy.data.cassandra.converters

import com.datastax.spark.connector.types._
import com.mindcandy.data.kryo.KryoCache
import com.twitter.algebird.HLL
import java.nio.ByteBuffer
import scala.reflect.runtime.universe._

trait AnyToHyperLogLogConverter extends TypeConverter[HLL] {
  def targetTypeTag: TypeTag[HLL] = typeTag[HLL]

  def convertPF: PartialFunction[Any, HLL] = {
    case bytes: Array[Byte] => KryoCache.fromBytes[HLL](bytes)
    case bytes: ByteBuffer => KryoCache.fromBytes[HLL](bytes.array())
  }
}

object AnyToHyperLogLogConverter extends AnyToHyperLogLogConverter

trait HyperLogLogToArrayByteConverter extends TypeConverter[Array[Byte]] {
  def targetTypeTag: TypeTag[Array[Byte]] = typeTag[Array[Byte]]

  def convertPF: PartialFunction[Any, Array[Byte]] = {
    case counter: HLL => KryoCache.toBytes(counter)
  }
}

object HyperLogLogToArrayByteConverter extends HyperLogLogToArrayByteConverter

trait HyperLogLogToByteBufferConverter extends TypeConverter[ByteBuffer] {
  def targetTypeTag: TypeTag[ByteBuffer] = typeTag[ByteBuffer]

  def convertPF: PartialFunction[Any, ByteBuffer] = {
    case counter: HLL => ByteBuffer.wrap(KryoCache.toBytes(counter))
  }
}

object HyperLogLogToByteBufferConverter extends HyperLogLogToByteBufferConverter