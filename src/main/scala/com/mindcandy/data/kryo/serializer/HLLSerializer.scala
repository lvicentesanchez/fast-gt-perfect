package com.mindcandy.data.kryo.serializer

import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.esotericsoftware.kryo.io.{ Input, Output }
import com.twitter.algebird.{ HLL, HyperLogLog }

class HLLSerializer extends Serializer[HLL] {
  setAcceptsNull(false)
  setImmutable(true)

  def write(kryo: Kryo, output: Output, value: HLL): Unit = {
    val bytes: Array[Byte] = HyperLogLog.toBytes(value)
    output.writeInt(bytes.length)
    output.writeBytes(bytes)
  }

  def read(kryo: Kryo, input: Input, clazz: Class[HLL]): HLL = {
    val length: Int = input.readInt()
    HyperLogLog.fromBytes(input.readBytes(length))
  }
}
