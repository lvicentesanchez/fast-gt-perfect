package com.mindcandy.data.kryo

import com.datastax.spark.connector.types.TypeConverter
import com.mindcandy.data.cassandra.converters._
import com.mindcandy.data.model.Amount
import com.twitter.algebird._
import java.util.Date
import org.joda.time.DateTime
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Prop
import org.scalacheck.Prop._
import org.specs2.{ ScalaCheck, Specification }
import org.specs2.time.NoTimeConversions

class TypeConvertersSpec extends Specification with ScalaCheck with NoTimeConversions {

  val bloomMonoid: BloomFilterMonoid = BloomFilter(10000, 0.001)
  val hyperMonoid: HyperLogLogMonoid = new HyperLogLogMonoid(12)
  val kryoCache: KryoCache = KryoCache()

  val converters: Seq[TypeConverter[_]] =
    Seq(
      AmountToIntConverter,
      AnyToAmountConverter,
      AnyToBloomFilterConverter(kryoCache),
      AnyToDateTimeConverter,
      BloomFilterToArrayByteConverter(kryoCache),
      AnyToHyperLogLogConverter(kryoCache),
      DateTimeToDateConverter,
      DateTimeToLongConverter,
      HyperLogLogToArrayByteConverter(kryoCache)
    )

  converters.foreach(TypeConverter.registerConverter)

  def is = sequential ^
    s2"""
    TypeConvertersSpec
    ==================

      Amount Converters
      -----------------

        It should convert an Amount to Int and back to Amount     ${amountToInt()}

      BloomFilter Converters
      ----------------------

        It should convert a BF to Array[Byte] and back to BF      ${bloomToArrayByte()}

      DateTime Converters
      ----------------------

        It should convert a DateTime to Date and back to DateTime ${dateTimeToDate()}
        It should convert a DateTime to Long and back to DateTime ${dateTimeToLong()}

      HyperLogLog Converters
      ----------------------

        It should convert a HLL to Array[Byte] and back to HLL    ${hyperToArrayByte()}

      SpaceSaver Converters
      ----------------------

        It should convert a SS to Array[Byte] and back to SS      ${hyperToArrayByte()}
    """

  def amountToInt(): Prop = forAllNoShrink(Gen.oneOf(negNum[Int], posNum[Int])) { num =>
    val amount: Amount = Amount(num)
    val tempor: Int = TypeConverter.forType[Int].convert(amount)
    val result: Amount = TypeConverter.forType[Amount].convert(tempor)

    result must_== amount
  }

  def bloomToArrayByte(): Prop = forAllNoShrink(nonEmptyListOf(uuid.map(_.toString))) { users =>
    val bloom: BF = bloomMonoid.create(users: _*)
    val tempo: Array[Byte] = TypeConverter.forType[Array[Byte]].convert(bloom)
    val resul: BF = TypeConverter.forType[BF].convert(tempo)

    resul must_== bloom
  }

  def dateTimeToDate() = {
    val now: DateTime = DateTime.now()
    val tmp: Date = TypeConverter.forType[Date].convert(now)
    val res: DateTime = TypeConverter.forType[DateTime].convert(tmp)
    res must_== now
  }

  def dateTimeToLong() = {
    val now: DateTime = DateTime.now()
    val res: Long = TypeConverter.forType[Long].convert(now)
    res must_== now.getMillis
  }

  def hyperToArrayByte(): Prop = forAllNoShrink(nonEmptyListOf(uuid.map(_.toString))) { users =>
    val hyper: HLL = hyperMonoid.batchCreate(users)(_.getBytes)
    val tempo: Array[Byte] = TypeConverter.forType[Array[Byte]].convert(hyper)
    val resul: HLL = TypeConverter.forType[HLL].convert(tempo)

    resul must_== hyper
  }

  def spaceToArrayByte(): Prop = forAllNoShrink(nonEmptyListOf(uuid.map(_.toString))) { users =>
    val space: SpaceSaver[String] = users.map(SpaceSaver(200, _)).reduce(_ ++ _)
    val tempo: Array[Byte] = TypeConverter.forType[Array[Byte]].convert(space)
    val resul: SpaceSaver[String] = TypeConverter.forType[SpaceSaver[String]].convert(tempo)

    resul must_== space
  }

}
