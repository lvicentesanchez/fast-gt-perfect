package com.mindcandy.data.cassandra.converters

import com.datastax.spark.connector.types._
import com.mindcandy.data.kryo.KryoCache
import com.twitter.algebird.BF
import java.nio.ByteBuffer
import scala.reflect.runtime.universe._

trait AnyToBloomFilterConverter extends TypeConverter[BF] {
  def targetTypeTag: TypeTag[BF] = typeTag[BF]

  def convertPF: PartialFunction[Any, BF] = {
    case bytes: Array[Byte] => KryoCache.fromBytes[BF](bytes)
    case bytes: ByteBuffer => KryoCache.fromBytes[BF](bytes.array())
  }
}

object AnyToBloomFilterConverter extends AnyToBloomFilterConverter

trait BloomFilterToArrayByteConverter extends TypeConverter[Array[Byte]] {
  def targetTypeTag: TypeTag[Array[Byte]] = typeTag[Array[Byte]]

  def convertPF: PartialFunction[Any, Array[Byte]] = {
    case bloom: BF => KryoCache.toBytes(bloom)
  }
}

object BloomFilterToArrayByteConverter extends BloomFilterToArrayByteConverter

trait BloomFilterToByteBufferConverter extends TypeConverter[ByteBuffer] {
  def targetTypeTag: TypeTag[ByteBuffer] = typeTag[ByteBuffer]

  def convertPF: PartialFunction[Any, ByteBuffer] = {
    case bloom: BF => ByteBuffer.wrap(KryoCache.toBytes(bloom))
  }
}

object BloomFilterToByteBufferConverter extends BloomFilterToByteBufferConverter
