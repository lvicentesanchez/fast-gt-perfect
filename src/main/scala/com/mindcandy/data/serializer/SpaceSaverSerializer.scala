package com.mindcandy.data.serializer

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.twitter.algebird.{ SpaceSaver, SSMany, SSOne }
import scala.collection.immutable.SortedMap

class SpaceSaverSerializer[T] extends Serializer[SpaceSaver[T]] {
  setAcceptsNull(false)
  setImmutable(true)

  type Buckets = SortedMap[Long, Set[T]]
  type Counters = Map[T, (Long, Long)]

  override def write(kryo: Kryo, output: Output, value: SpaceSaver[T]): Unit =
    value match {
      case SSOne(capacity, item) =>
        output.writeByte(SpaceSaverSerializer.SSOne)
        output.writeInt(capacity)
        kryo.writeClassAndObject(output, value)

      case SSMany(capacity, counters, buckets) =>
        output.writeByte(SpaceSaverSerializer.SSMany)
        output.writeInt(capacity)
        kryo.writeObject(output, counters)
        kryo.writeObject(output, buckets)
    }

  override def read(kryo: Kryo, input: Input, clazz: Class[SpaceSaver[T]]): SpaceSaver[T] = {
    input.readByte() match {
      case SpaceSaverSerializer.SSOne =>
        SSOne(input.readInt(), kryo.readClassAndObject(input).asInstanceOf[T])
      case SpaceSaverSerializer.SSMany =>
        SSMany(input.readInt(), kryo.readObject(input, classOf[Counters]), kryo.readObject(input, classOf[Buckets]))
    }
  }
}

object SpaceSaverSerializer {
  final val SSOne: Byte = 0
  final val SSMany: Byte = 1
}
