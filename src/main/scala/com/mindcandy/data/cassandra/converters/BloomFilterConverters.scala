package com.mindcandy.data.cassandra.converters

import com.datastax.spark.connector.types._
import com.esotericsoftware.kryo.io.{ Output, Input }
import com.mindcandy.data.kryo.KryoCache
import com.twitter.algebird.BF
import java.nio.ByteBuffer
import scala.reflect.runtime.universe._

class AnyToBloomFilterConverter(cache: KryoCache) extends TypeConverter[BF] {
  def targetTypeTag: TypeTag[BF] = typeTag[BF]

  def convertPF: PartialFunction[Any, BF] = {
    case bytes: Array[Byte] => cache.withKryoInstance(_.readObject(new Input(bytes), classOf[BF]))
    case bytes: ByteBuffer => cache.withKryoInstance(_.readObject(new Input(bytes.array()), classOf[BF]))
  }
}

object AnyToBloomFilterConverter {
  def apply(cache: KryoCache): AnyToBloomFilterConverter = new AnyToBloomFilterConverter(cache)
}

class BloomFilterToArrayByteConverter(cache: KryoCache) extends TypeConverter[Array[Byte]] {
  def targetTypeTag: TypeTag[Array[Byte]] = typeTag[Array[Byte]]

  def convertPF: PartialFunction[Any, Array[Byte]] = {
    case bloom: BF =>
      val output: Output = new Output(4096, -1)
      cache.withKryoInstance(_.writeObject(output, bloom))
      output.toBytes
  }
}

object BloomFilterToArrayByteConverter {
  def apply(cache: KryoCache): BloomFilterToArrayByteConverter = new BloomFilterToArrayByteConverter(cache)
}
