package com.mindcandy.data.cassandra.converters

import com.datastax.spark.connector.types._
import com.esotericsoftware.kryo.io.{ Input, Output }
import com.mindcandy.data.kryo.KryoCache
import com.twitter.algebird.HLL
import java.nio.ByteBuffer
import scala.reflect.runtime.universe._

class AnyToHyperLogLogConverter(cache: KryoCache) extends TypeConverter[HLL] {
  def targetTypeTag: TypeTag[HLL] = typeTag[HLL]

  def convertPF: PartialFunction[Any, HLL] = {
    case bytes: Array[Byte] => cache.withKryoInstance(_.readObject(new Input(bytes), classOf[HLL]))
    case bytes: ByteBuffer => cache.withKryoInstance(_.readObject(new Input(bytes.array()), classOf[HLL]))
  }
}

object AnyToHyperLogLogConverter {
  def apply(cache: KryoCache): AnyToHyperLogLogConverter = new AnyToHyperLogLogConverter(cache)
}

class HyperLogLogToArrayByteConverter(bits: Int, offset: Int, cache: KryoCache) extends TypeConverter[Array[Byte]] {
  def targetTypeTag: TypeTag[Array[Byte]] = typeTag[Array[Byte]]

  def convertPF: PartialFunction[Any, Array[Byte]] = {
    case counter: HLL =>
      val output: Output = new Output((1 << bits) + 1024)
      cache.withKryoInstance(_.writeObject(output, counter))
      output.toBytes
  }
}

object HyperLogLogToArrayByteConverter {
  def apply(bits: Int, offset: Int, cache: KryoCache): HyperLogLogToArrayByteConverter =
    new HyperLogLogToArrayByteConverter(bits, offset, cache)
}
