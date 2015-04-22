package com.mindcandy.data.kryo.serializer

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.googlecode.javaewah.{ EWAHCompressedBitmap => CBitSet }
import java.io.{ DataInputStream, DataOutputStream }
import scalaz.syntax.id._

class CBitSetSerializer extends Serializer[CBitSet] {
  setAcceptsNull(false)
  setImmutable(true)

  def write(kryo: Kryo, output: Output, value: CBitSet): Unit =
    value.serialize(new DataOutputStream(output))

  def read(kryo: Kryo, input: Input, clazz: Class[CBitSet]): CBitSet =
    new CBitSet() <| (bitset => bitset.deserialize(new DataInputStream(input)))
}
