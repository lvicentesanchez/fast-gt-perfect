package com.mindcandy.data.cassandra.converters

import com.datastax.spark.connector.types._
import com.mindcandy.data.kryo.KryoCache
import com.twitter.algebird.SpaceSaver
import java.nio.ByteBuffer
import scala.reflect.runtime.universe._

class AnyToSpaceSaverConverter[T: TypeTag]() extends TypeConverter[SpaceSaver[T]] {
  def targetTypeTag: TypeTag[SpaceSaver[T]] = typeTag[SpaceSaver[T]]

  def convertPF: PartialFunction[Any, SpaceSaver[T]] = {
    case bytes: Array[Byte] => KryoCache.fromBytes[SpaceSaver[T]](bytes)
    case bytes: ByteBuffer => KryoCache.fromBytes[SpaceSaver[T]](bytes.array())
  }
}

object AnyToSpaceSaverConverter extends AnyToSpaceSaverConverter {
  def apply[T: TypeTag](): AnyToSpaceSaverConverter[T] = new AnyToSpaceSaverConverter[T]()
}

trait SpaceSaverToArrayByteConverter extends TypeConverter[Array[Byte]] {
  def targetTypeTag: TypeTag[Array[Byte]] = typeTag[Array[Byte]]

  def convertPF: PartialFunction[Any, Array[Byte]] = {
    case saver: SpaceSaver[_] => KryoCache.toBytes(saver)
  }
}

object SpaceSaverToArrayByteConverter extends SpaceSaverToArrayByteConverter
