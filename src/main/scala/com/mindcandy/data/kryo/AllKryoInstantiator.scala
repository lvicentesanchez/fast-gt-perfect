package com.mindcandy.data.kryo

import com.mindcandy.data.kryo.serializer.AlgebirdRegistrator
import com.twitter.chill.{ AllScalaRegistrar, EmptyScalaKryoInstantiator }

class AllKryoInstantiator() extends EmptyScalaKryoInstantiator {
  override def newKryo() = {
    val k = super.newKryo()
    val reg = new AllScalaRegistrar
    val alg = new AlgebirdRegistrator
    alg.registerClasses(k)
    reg(k)
    k
  }
}

object AllKryoInstantiator {
  def apply(): AllKryoInstantiator = new AllKryoInstantiator()
}
