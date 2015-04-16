package com.mindcandy.data.cassandra.converters

import com.datastax.spark.connector.types._
import com.esotericsoftware.kryo.io.{ Output, Input }
import com.mindcandy.data.kryo.KryoCache
import com.twitter.algebird.SpaceSaver
import java.nio.ByteBuffer
import scala.reflect.runtime.universe._

class AnyToSpaceSaverConverter[T: TypeTag](cache: KryoCache) extends TypeConverter[SpaceSaver[T]] {
  def targetTypeTag: TypeTag[SpaceSaver[T]] = typeTag[SpaceSaver[T]]

  def convertPF: PartialFunction[Any, SpaceSaver[T]] = {
    case bytes: Array[Byte] => cache.withKryoInstance(_.readObject(new Input(bytes), classOf[SpaceSaver[T]]))
    case bytes: ByteBuffer => cache.withKryoInstance(_.readObject(new Input(bytes.array()), classOf[SpaceSaver[T]]))
  }
}

object AnyToSpaceSaverConverter {
  def apply[T: TypeTag](cache: KryoCache): AnyToSpaceSaverConverter[T] = new AnyToSpaceSaverConverter[T](cache)
}

class SpaceSaverToArrayByteConverter(cache: KryoCache) extends TypeConverter[Array[Byte]] {
  def targetTypeTag: TypeTag[Array[Byte]] = typeTag[Array[Byte]]

  def convertPF: PartialFunction[Any, Array[Byte]] = {
    case saver: SpaceSaver[_] =>
      Array[Byte]()
      val output: Output = new Output(32 * 1024)
      cache.withKryoInstance(_.writeObject(output, saver))
      output.toBytes
  }
}

object SpaceSaverToArrayByteConverter {
  def apply(cache: KryoCache): SpaceSaverToArrayByteConverter = new SpaceSaverToArrayByteConverter(cache)
}
