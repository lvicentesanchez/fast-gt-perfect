package com.mindcandy.data.kryo.serializer

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.twitter.algebird.BFHash

class BFHashSerializer extends Serializer[BFHash] {
  setAcceptsNull(false)
  setImmutable(true)

  override def write(kryo: Kryo, output: Output, value: BFHash): Unit = {
    output.writeInt(value.numHashes)
    output.writeInt(value.width)
    output.writeLong(value.seed)
  }

  override def read(kryo: Kryo, input: Input, clazz: Class[BFHash]): BFHash =
    BFHash(input.readInt(), input.readInt(), input.readLong())
}
