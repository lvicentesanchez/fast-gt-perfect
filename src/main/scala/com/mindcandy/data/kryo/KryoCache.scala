package com.mindcandy.data.kryo

import com.twitter.chill.{ KryoInstantiator, KryoPool }
import scala.reflect.ClassTag

trait KryoCache extends Serializable {
  def kryoInstantiator: KryoInstantiator
  def kryoPool: KryoPool
  def fromBytes[T](bytes: Array[Byte])(implicit ct: ClassTag[T]): T
  def toBytes[T](obj: T): Array[Byte]
}

object KryoCache extends KryoCache {
  override val kryoInstantiator: KryoInstantiator = AllKryoInstantiator()

  override val kryoPool: KryoPool =
    KryoPool.withBuffer(10, kryoInstantiator, 4096, -1)

  override def fromBytes[T](bytes: Array[Byte])(implicit ct: ClassTag[T]): T =
    kryoPool.fromBytes(bytes, ct.runtimeClass.asInstanceOf[Class[T]])

  override def toBytes[T](obj: T): Array[Byte] = kryoPool.toBytesWithoutClass(obj)
}
