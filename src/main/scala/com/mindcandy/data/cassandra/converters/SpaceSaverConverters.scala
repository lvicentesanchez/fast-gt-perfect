package com.mindcandy.data.cassandra.converters

import com.datastax.spark.connector.types._
import com.twitter.algebird.SpaceSaver
import com.twitter.algebird.extensions._
import java.nio.ByteBuffer
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

trait AnyToSpaceSaverConverter extends TypeConverter[SpaceSaver[String]] {
  def targetTypeTag: TypeTag[SpaceSaver[String]] = typeTag[SpaceSaver[String]]

  def convertPF: PartialFunction[Any, SpaceSaver[String]] = {
    case bytes: Array[Byte] => SpaceSaver.fromBytes(bytes)
    case bytes: ByteBuffer => SpaceSaver.fromByteBuffer(bytes)
  }
}

object AnyToSpaceSaverConverter extends AnyToSpaceSaverConverter

class SpaceSaverToArrayByteConverter[T: ClassTag] extends TypeConverter[Array[Byte]] {
  def targetTypeTag: TypeTag[Array[Byte]] = typeTag[Array[Byte]]

  def convertPF: PartialFunction[Any, Array[Byte]] = {
    case source: SpaceSaver[T] => Array[Byte]()
  }
}
