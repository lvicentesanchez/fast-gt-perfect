package com.mindcandy.data.kryo.serializer

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.googlecode.javaewah.{ EWAHCompressedBitmap => CBitSet }
import com.twitter.algebird._
import scala.collection.immutable.BitSet

class BFSerializer extends Serializer[BF] {
  setAcceptsNull(false)
  setImmutable(true)

  override def write(kryo: Kryo, output: Output, value: BF): Unit =
    value match {
      case BFZero(hashes, width) =>
        output.writeByte(BFSerializer.BFZero)
        kryo.writeObject(output, hashes)
        output.writeInt(width)

      case BFItem(item, hashes, width) =>
        output.writeByte(BFSerializer.BFItem)
        output.writeString(item)
        kryo.writeObject(output, hashes)
        output.writeInt(width)

      case BFInstance(hashes, bits, width) =>
        output.writeByte(BFSerializer.BFInstance)
        kryo.writeObject(output, hashes)
        kryo.writeObject(output, bits)
        output.writeInt(width)

      case BFSparse(hashes, bits, width) =>
        output.writeByte(BFSerializer.BFSparse)
        kryo.writeObject(output, hashes)
        kryo.writeObject(output, bits)
        output.writeInt(width)
    }

  override def read(kryo: Kryo, input: Input, clazz: Class[BF]): BF = {
    input.readByte() match {
      case BFSerializer.BFZero =>
        BFZero(kryo.readObject(input, classOf[BFHash]), input.readInt())
      case BFSerializer.BFItem =>
        BFItem(input.readString(), kryo.readObject(input, classOf[BFHash]), input.readInt())
      case BFSerializer.BFInstance =>
        BFInstance(kryo.readObject(input, classOf[BFHash]), kryo.readObject(input, classOf[BitSet]), input.readInt())
      case BFSerializer.BFSparse =>
        BFSparse(kryo.readObject(input, classOf[BFHash]), kryo.readObject(input, classOf[CBitSet]), input.readInt())
    }
  }
}

object BFSerializer {
  final val BFZero: Byte = 0
  final val BFItem: Byte = 1
  final val BFInstance: Byte = 2
  final val BFSparse: Byte = 3
}
