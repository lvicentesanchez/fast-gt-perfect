package com.mindcandy.data.kryo.serializer

import com.googlecode.javaewah.{ EWAHCompressedBitmap => CBitSet }
import com.twitter.algebird._
import com.twitter.chill.Kryo
import org.apache.spark.serializer.KryoRegistrator

class AlgebirdRegistrator extends KryoRegistrator {
  def registerClasses(kryo: Kryo) {
    kryo.register(classOf[BF], new BFSerializer())
    kryo.register(classOf[BFZero], new BFSerializer())
    kryo.register(classOf[BFItem], new BFSerializer())
    kryo.register(classOf[BFInstance], new BFSerializer())
    kryo.register(classOf[BFSparse], new BFSerializer())
    kryo.register(classOf[BFHash], new BFHashSerializer())
    kryo.register(classOf[CBitSet], new CBitSetSerializer())
    kryo.register(classOf[HLL], new HLLSerializer())
    kryo.register(classOf[DenseHLL], new HLLSerializer())
    kryo.register(classOf[SparseHLL], new HLLSerializer())
    kryo.register(classOf[SpaceSaver[AnyRef]], new SpaceSaverSerializer[AnyRef]())
    kryo.register(classOf[SSOne[AnyRef]], new SpaceSaverSerializer[AnyRef]())
    kryo.register(classOf[SSMany[AnyRef]], new SpaceSaverSerializer[AnyRef]())
  }
}
