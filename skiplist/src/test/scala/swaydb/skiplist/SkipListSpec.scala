/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.skiplist

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.slice.order.KeyOrder
import swaydb.slice.{Slice, SliceOption}
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.testkit.RunThis._
import swaydb.{Bag, Glass}

import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.parallel.CollectionConverters._
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.util.{Random, Try}

class Concurrent_SkipListSpec extends SkipListSpec {
  override def create[NK, NV, K <: NK, V <: NV](nullKey: NK, nullValue: NV)(implicit keyOrder: KeyOrder[K]): SkipList[NK, NV, K, V] =
    SkipListConcurrent[NK, NV, K, V](nullKey, nullValue)
}

class TreeMap_SkipListSpec extends SkipListSpec {
  override def create[NK, NV, K <: NK, V <: NV](nullKey: NK, nullValue: NV)(implicit keyOrder: KeyOrder[K]): SkipList[NK, NV, K, V] =
    SkipListTreeMap[NK, NV, K, V](nullKey, nullValue)
}

class Series_Length10_SkipListSpec extends SkipListSpec {
  override def create[NK, NV, K <: NK, V <: NV](nullKey: NK, nullValue: NV)(implicit keyOrder: KeyOrder[K]): SkipList[NK, NV, K, V] =
    SkipListSeries[NK, NV, K, V](lengthPerSeries = 10, nullKey = nullKey, nullValue = nullValue)
}

class Series_Length1_SkipListSpec extends SkipListSpec {
  override def create[NK, NV, K <: NK, V <: NV](nullKey: NK, nullValue: NV)(implicit keyOrder: KeyOrder[K]): SkipList[NK, NV, K, V] =
    SkipListSeries[NK, NV, K, V](lengthPerSeries = 1, nullKey = nullKey, nullValue = nullValue)
}

sealed trait SkipListSpec extends AnyWordSpec with Matchers {

  sealed trait ValueOption
  object Value {
    final case object Null extends ValueOption
    case class Some(value: Int) extends ValueOption
  }

  implicit val ordering = KeyOrder.default

  def create[NK, NV, K <: NK, V <: NV](nullKey: NK, nullValue: NV)(implicit keyOrder: KeyOrder[K]): SkipList[NK, NV, K, V]

  def create(): SkipList[SliceOption[Byte], ValueOption, Slice[Byte], Value.Some] =
    create[SliceOption[Byte], ValueOption, Slice[Byte], Value.Some](Slice.Null, Value.Null)

  "put" in {
    val skipList = create()
    skipList.put(1, Value.Some(1))
    skipList.get(1) shouldBe Value.Some(1)

    skipList.put(1, Value.Some(2))
    skipList.get(1) shouldBe Value.Some(2)
  }

  "putIfAbsent" in {
    val skipList = create()
    if (skipList.isInstanceOf[SkipListTreeMap[_, _, _, _]])
      cancel("Test does not apply for TreeMap")

    skipList.put(1, Value.Some(1))
    skipList.get(1) shouldBe Value.Some(1)

    skipList.putIfAbsent(1, Value.Some(123)) shouldBe false
    skipList.get(1) shouldBe Value.Some(1)

    skipList.putIfAbsent(2, Value.Some(22)) shouldBe true
    skipList.get(2) shouldBe Value.Some(22)
  }

  "get" in {
    val skipList = create()
    skipList.get(1) shouldBe Value.Null

    skipList.put(11, Value.Some(111))
    skipList.get(11) shouldBe Value.Some(111)
  }

  "remove" in {
    val skipList = create()
    if (skipList.isInstanceOf[SkipListTreeMap[_, _, _, _]])
      cancel("Test does not apply for TreeMap")

    skipList.put(11, Value.Some(111))
    skipList.get(11) shouldBe Value.Some(111)

    skipList.isEmpty shouldBe false
    skipList.remove(11)
    skipList.isEmpty shouldBe true

    skipList.get(11) shouldBe Value.Null
  }

  "lower & lowerKey" in {
    val skipList = create()
    skipList.put(1, Value.Some(1))

    skipList.lower(2) shouldBe Value.Some(1)
    skipList.lower(1) shouldBe Value.Null
    skipList.lower(0) shouldBe Value.Null

    skipList.lowerKey(2) shouldBe (1: Slice[Byte])
    skipList.lowerKey(1) shouldBe Slice.Null
    skipList.lowerKey(0) shouldBe Slice.Null
  }

  "floor" in {
    val skipList = create()
    skipList.put(1, Value.Some(1))

    skipList.floor(1) shouldBe Value.Some(1)
    skipList.floor(0) shouldBe Value.Null
  }

  "higher" in {
    val skipList = create()
    skipList.put(1, Value.Some(1))

    skipList.higher(0) shouldBe Value.Some(1)
    skipList.higher(1) shouldBe Value.Null
  }

  "ceiling & ceilingKey" in {
    val skipList = create()
    skipList.put(1, Value.Some(1))

    skipList.ceiling(0) shouldBe Value.Some(1)
    skipList.ceiling(1) shouldBe Value.Some(1)
    skipList.ceiling(2) shouldBe Value.Null

    skipList.ceilingKey(0) shouldBe (1: Slice[Byte])
    skipList.ceilingKey(1) shouldBe (1: Slice[Byte])
    skipList.ceilingKey(2) shouldBe Slice.Null
  }

  "head, last, headKey & lastKey" in {
    val skipList = create()
    skipList.put(1, Value.Some(1))
    skipList.put(2, Value.Some(2))

    skipList.head() shouldBe Value.Some(1)
    skipList.last() shouldBe Value.Some(2)
    skipList.headKey shouldBe (1: Slice[Byte])
    skipList.lastKey shouldBe (2: Slice[Byte])

    skipList.isEmpty shouldBe false
    skipList.clear()
    skipList.isEmpty shouldBe true

    skipList.head() shouldBe Value.Null
    skipList.last() shouldBe Value.Null
    skipList.headKey shouldBe Slice.Null
    skipList.lastKey shouldBe Slice.Null
  }

  "random search" in {
    val skipList = create()
    val max = 10
    val range = 1 to max

    val random = Random.shuffle(range.toList)

    random foreach {
      i =>
        skipList.put(i, Value.Some(i))
    }

    skipList.toIterable.toList.map(_._2) shouldBe range.map(i => Value.Some(i))

    //assert lower
    range.foldLeft(Value.Null: ValueOption) {
      case (expectedLower, next) =>
        val lower = skipList.lower(next)
        lower shouldBe expectedLower
        Value.Some(next)
    }

    //assert higher
    range.foldRight(Value.Null: ValueOption) {
      case (next, expectedHigher) =>
        val higher = skipList.higher(next)
        higher shouldBe expectedHigher
        Value.Some(next)
    }

    //floor and ceiling
    range.foreach {
      int =>
        skipList.floor(int) shouldBe Value.Some(int)
        skipList.ceiling(int) shouldBe Value.Some(int)
    }
  }

  "subMap" when {
    "empty" in {
      val skipList = create()

      (1 to 10) foreach {
        i =>
          skipList.subMap(from = 1, fromInclusive = true, to = i, toInclusive = true) shouldBe empty
          skipList.subMap(from = 1, fromInclusive = true, to = i, toInclusive = false) shouldBe empty
          skipList.subMap(from = 1, fromInclusive = false, to = i, toInclusive = true) shouldBe empty
          skipList.subMap(from = 1, fromInclusive = false, to = i, toInclusive = false) shouldBe empty
      }
    }

    "inconsistentRanges" in {
      val skipList = create()

      assertThrows[IllegalArgumentException](skipList.subMap(from = 1, fromInclusive = true, to = 0, toInclusive = true))
      assertThrows[IllegalArgumentException](skipList.subMap(from = 1, fromInclusive = true, to = 0, toInclusive = false))
      assertThrows[IllegalArgumentException](skipList.subMap(from = 1, fromInclusive = false, to = 0, toInclusive = true))
      assertThrows[IllegalArgumentException](skipList.subMap(from = 1, fromInclusive = false, to = 0, toInclusive = false))
    }

//    "result same as ConcurrentSkipListMap" in {
//      runThis(100.times, log = true) {
//        val scalaSkipList = create()
//        val javaSkipList = new ConcurrentSkipListMap[Slice[Byte], Value.Some](ordering)
//
//        val max = randomIntMax(100)
//
//        (1 to max) foreach {
//          i =>
//            scalaSkipList.put(i, Value.Some(i))
//            javaSkipList.put(i, Value.Some(i))
//        }
//
//        runThis(10.times) {
//          val from = eitherOne(0, randomIntMax(max max 1))
//          val to = eitherOne(from, from + 1, randomIntMax(max max 1) max from, from + randomIntMax(max max 1))
//
//          runThis(20.times) {
//            val fromInclusive = randomBoolean()
//            val toInclusive = randomBoolean()
//
//            val scalaResult: List[(Slice[Byte], Value.Some)] = scalaSkipList.subMap(from, fromInclusive, to, toInclusive).toList
//            val javaResult: List[(Slice[Byte], Value.Some)] = javaSkipList.subMap(from, fromInclusive, to, toInclusive).asScala.toList
//
//            scalaResult shouldBe javaResult
//          }
//        }
//      }
//    }
  }

//  "transaction" should {
//
//    def runTest[BAG[_]](await: BAG[Unit] => Unit)(implicit bag: Bag[BAG]) = {
//      val skipList = create[Int, AtomicBoolean, Int, AtomicBoolean](Int.MinValue, null)(KeyOrder(Ordering.Int))
//
//      skipList.put(1, new AtomicBoolean(false))
//
//      (1 to 1000).par foreach {
//        _ =>
//          val result =
//            skipList.atomicWrite(from = 1, to = 1, toInclusive = true) {
//              val boolean = skipList.get(1)
//
//              //when this code block is executed boolean is always false!
//              boolean.get() shouldBe false
//              boolean.set(true)
//
//              //allow some time for threads to concurrently access this value
//              eitherOne(sleep(randomIntMax(10).milliseconds), ())
//
//              boolean.set(false)
//            }
//
//          await(result)
//      }
//    }
//
//    "not allow concurrent updated" when {
//      //run for each bag
//
//      "glass" in {
//        runTest[Glass](result => result)
//      }
//
//      "try" in {
//        runTest[Try](_.get)
//      }
//
//      "future" in {
//        runTest[Future](_.await(10.seconds))(Bag.future(TestExecutionContext.executionContext))
//      }
//    }
//  }
}
