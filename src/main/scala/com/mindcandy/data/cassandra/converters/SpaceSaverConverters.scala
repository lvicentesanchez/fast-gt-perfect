package com.mindcandy.data.cassandra.converters

import com.datastax.spark.connector.types._
import com.mindcandy.data.kryo.KryoCache
import com.twitter.algebird.SpaceSaver
import java.nio.ByteBuffer
import scala.reflect.runtime.universe._

trait AnyToSpaceSaverStringConverter extends TypeConverter[SpaceSaver[String]] {
  def targetTypeTag: TypeTag[SpaceSaver[String]] = typeTag[SpaceSaver[String]]

  def convertPF: PartialFunction[Any, SpaceSaver[String]] = {
    case bytes: Array[Byte] => KryoCache.fromBytes[SpaceSaver[String]](bytes)
    case bytes: ByteBuffer => KryoCache.fromBytes[SpaceSaver[String]](bytes.array())
  }
}

object AnyToSpaceSaverStringConverter extends AnyToSpaceSaverStringConverter

trait SpaceSaverToArrayByteConverter extends TypeConverter[Array[Byte]] {
  def targetTypeTag: TypeTag[Array[Byte]] = typeTag[Array[Byte]]

  def convertPF: PartialFunction[Any, Array[Byte]] = {
    case saver: SpaceSaver[_] => KryoCache.toBytes(saver)
  }
}

object SpaceSaverToArrayByteConverter extends SpaceSaverToArrayByteConverter

trait SpaceSaverToByteBufferConverter extends TypeConverter[ByteBuffer] {
  def targetTypeTag: TypeTag[ByteBuffer] = typeTag[ByteBuffer]

  def convertPF: PartialFunction[Any, ByteBuffer] = {
    case saver: SpaceSaver[_] => ByteBuffer.wrap(KryoCache.toBytes(saver))
  }
}

object SpaceSaverToByteBufferConverter extends SpaceSaverToByteBufferConverter
