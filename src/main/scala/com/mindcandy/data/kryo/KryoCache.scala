package com.mindcandy.data.kryo

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.pool.{ KryoPool, KryoFactory }
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer

class KryoCache(sparkConf: SparkConf) {
  val kryoInstance: KryoSerializer = new KryoSerializer(sparkConf)
  val kryoFactory: KryoFactory = new KryoFactory() {
    def create(): Kryo = kryoInstance.newKryo()
  }
  val kryoPool: KryoPool = new KryoPool.Builder(kryoFactory).softReferences().build()

  def withKryoInstance[T](f: Kryo => T): T = {
    val kryo: Kryo = kryoPool.borrow()
    val output: T = f(kryo)
    kryoPool.release(kryo)
    output
  }
}
