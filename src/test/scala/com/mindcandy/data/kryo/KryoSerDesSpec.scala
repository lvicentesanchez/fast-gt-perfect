package com.mindcandy.data.kryo

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.twitter.algebird._
import org.scalacheck.Prop
import org.scalacheck.Gen._
import org.scalacheck.Prop._
import org.apache.spark.SparkConf
import org.specs2.Specification
import org.specs2.execute.{ AsResult, Result }
import org.specs2.specification.FixtureExample
import org.specs2.time.NoTimeConversions
import org.specs2.ScalaCheck

class KryoSerDesSpec extends Specification with ScalaCheck with NoTimeConversions {

  val hyperMonoid: HyperLogLogMonoid = new HyperLogLogMonoid(12)
  val bloomMonoid: BloomFilterMonoid = BloomFilter(10000, 0.001)

  val sparkConf: SparkConf =
    new SparkConf().
      setMaster("local[4]").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      set("spark.kryo.registrator", "com.mindcandy.data.kryo.serializer.AlgebirdRegistrator").
      setAppName(this.getClass.getSimpleName)

  val kryoCache: KryoCache = new KryoCache(sparkConf)

  def is = sequential ^
    s2"""
    KryoSerDesSpec
    ==============

      ser/des HLL
      -----------

        It should ser/der a HLL using Kryo ${serdesHyperLogLog()}

      ser/des BF
      -----------

        It should ser/der a BF using Kryo  ${serdesBloomFilter()}

      ser/des SS
      -----------

        It should ser/der a SS using Kryo  ${serdesSpaceSaver()}
    """

  def serdesHyperLogLog(): Prop = forAllNoShrink(nonEmptyListOf(uuid.map(_.toString))) { users =>
    val hyper: HLL = hyperMonoid.batchCreate(users)(_.getBytes)
    val resul: HLL =
      kryoCache.withKryoInstance(
        kryo => {
          val output: Output = new Output(4096, -1)
          kryo.writeObject(output, hyper)
          kryo.readObject(new Input(output.toBytes), classOf[HLL])
        }
      )

    resul must_== hyper
  }

  def serdesBloomFilter(): Prop = forAllNoShrink(nonEmptyListOf(uuid.map(_.toString))) { users =>
    val bloom: BF = bloomMonoid.create(users: _*)
    val resul: BF =
      kryoCache.withKryoInstance(
        kryo => {
          val output: Output = new Output(4096, -1)
          kryo.writeObject(output, bloom)
          kryo.readObject(new Input(output.toBytes), classOf[BF])
        }
      )

    resul must_== bloom
  }

  def serdesSpaceSaver(): Prop = forAllNoShrink(nonEmptyListOf(uuid.map(_.toString))) { users =>
    val saver: SpaceSaver[String] = users.map(SpaceSaver(200, _)).reduce(_ ++ _)
    val resul: SpaceSaver[String] =
      kryoCache.withKryoInstance(
        kryo => {
          val output: Output = new Output(4096, -1)
          kryo.writeObject(output, saver)
          kryo.readObject(new Input(output.toBytes), classOf[SpaceSaver[String]])
        }
      )

    resul must_== saver
  }

}
