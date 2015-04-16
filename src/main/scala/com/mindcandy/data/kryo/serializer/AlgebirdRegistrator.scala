package com.mindcandy.data.kryo.serializer

import com.googlecode.javaewah.{ EWAHCompressedBitmap => CBitSet }
import com.twitter.algebird._
import com.twitter.chill.Kryo
import org.apache.spark.serializer.KryoRegistrator

class AlgebirdRegistrator extends KryoRegistrator {
  def registerClasses(kryo: Kryo) {
    kryo.register(classOf[BF], new BFSerializer())
    kryo.register(classOf[BFHash], new BFHashSerializer())
    kryo.register(classOf[CBitSet], new CBitSetSerializer())
    kryo.register(classOf[HLL], new HLLSerializer())
    kryo.register(classOf[SpaceSaver[Any]], new SpaceSaverSerializer[Any]())
  }
}
