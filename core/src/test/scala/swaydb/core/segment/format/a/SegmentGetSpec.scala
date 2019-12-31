/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment.format.a

import org.scalatest.OptionValues._
import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.ScalaFutures
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.data._
import swaydb.core.segment.ThreadReadState
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.util.Random

class SegmentGetSpec0 extends SegmentGetSpec {
  val keyValuesCount = 1000
}

class SegmentGetSpec1 extends SegmentGetSpec {
  val keyValuesCount = 1000
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class SegmentGetSpec2 extends SegmentGetSpec {
  val keyValuesCount = 1000
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class SegmentGetSpec3 extends SegmentGetSpec {
  val keyValuesCount = 1000
  override def inMemoryStorage = true
}

sealed trait SegmentGetSpec extends TestBase with ScalaFutures with PrivateMethodTester {

  implicit val keyOrder = KeyOrder.default

  def keyValuesCount: Int

  "Segment.get" should {

    "fixed key-value" in {
      runThis(100.times, log = true) {
        assertSegment(
          keyValues = Slice(randomFixedKeyValue(1)),
          assert =
            (keyValues, segment) =>
              Random.shuffle(
                Seq(
                  () => segment.get(0, ThreadReadState.random).toOptional shouldBe empty,
                  () => segment.get(2, ThreadReadState.random).toOptional shouldBe empty,
                  () => segment.get(keyValues.head.key, ThreadReadState.random).getUnsafe shouldBe keyValues.head
                )
              ).foreach(_ ())
        )

        assertSegment(
          keyValues = Slice(randomFixedKeyValue(1), randomFixedKeyValue(2)),
          assert =
            (keyValues, segment) =>
              Random.shuffle(
                Seq(
                  () => segment.get(0, ThreadReadState.random).toOptional shouldBe empty,
                  () => segment.get(3, ThreadReadState.random).toOptional shouldBe empty,
                  () => segment.get(keyValues.head.key, ThreadReadState.random).getUnsafe shouldBe keyValues.head
                )
              ).foreach(_ ())
        )
      }
    }

    "range-value" in {
      runThis(100.times) {
        assertSegment(
          keyValues = Slice(randomRangeKeyValue(1, 10)),
          assert =
            (keyValues, segment) =>
              Random.shuffle(
                Seq(
                  () => segment.get(0, ThreadReadState.random).toOptional shouldBe empty,
                  () => segment.get(10, ThreadReadState.random).toOptional shouldBe empty,
                  () => segment.get(11, ThreadReadState.random).toOptional shouldBe empty,
                  () =>
                    (1 to 9) foreach {
                      i =>
                        segment.get(i).getUnsafe shouldBe keyValues.head
                    }
                )
              ).foreach(_ ())
        )

        assertSegment(
          keyValues =
            Slice(randomRangeKeyValue(1, 10), randomRangeKeyValue(10, 20)),
          assert =
            (keyValues, segment) =>
              Random.shuffle(
                Seq(
                  () => segment.get(0, ThreadReadState.random).toOptional shouldBe empty,
                  () => segment.get(20, ThreadReadState.random).toOptional shouldBe empty,
                  () => segment.get(21, ThreadReadState.random).toOptional shouldBe empty,
                  () =>
                    (1 to 9) foreach {
                      i =>
                        segment.get(i, ThreadReadState.random).getUnsafe shouldBe keyValues.head
                    },
                  () =>
                    (10 to 19) foreach {
                      i =>
                        segment.get(i).getUnsafe shouldBe keyValues.last
                    }
                )
              ).foreach(_ ())
        )
      }
    }

    "value random key-values" in {
      val keyValues = randomizedKeyValues(keyValuesCount)
      val segment = TestSegment(keyValues)
      assertGet(keyValues, segment)
    }

    "add unsliced key-values to Segment's caches" in {
      assertSegment(
        keyValues = randomizedKeyValues(keyValuesCount),
        testAgainAfterAssert = false,
        assert =
          (keyValues, segment) =>
            (0 until keyValues.size) foreach {
              index =>
                val keyValue = keyValues(index)
                if (persistent) segment.getFromCache(keyValue.key).toOptional shouldBe empty
                segment.get(keyValue.key).getUnsafe shouldBe keyValue

                val gotFromCache = eventually(segment.getFromCache(keyValue.key).getUnsafe)
                //underlying array sizes should not be slices but copies of arrays.
                gotFromCache.key.underlyingArraySize shouldBe keyValue.key.toArray.length

                gotFromCache match {
                  case range: KeyValue.Range =>
                    //if it's a range, toKey should also be unsliced.
                    range.toKey.underlyingArraySize shouldBe keyValues.find(_.key == range.fromKey).value.key.toArray.length
                  case _ =>
                    gotFromCache.getOrFetchValue.map(_.underlyingArraySize) shouldBe keyValue.getOrFetchValue.map(_.toArray.length)
                }
            }
      )
    }

    "add read key values to cache" in {
      runThis(20.times, log = true) {
        assertSegment(
          keyValues =
            randomizedKeyValues(keyValuesCount),

          testAgainAfterAssert =
            false,

          assert =
            (keyValues, segment) =>
              keyValues foreach {
                keyValue =>
                  if (persistent) segment isInKeyValueCache keyValue.key shouldBe false
                  (segment get keyValue.key).getUnsafe shouldBe keyValue
                  segment isInKeyValueCache keyValue.key shouldBe true
              }
        )
      }
    }

    //    "read value from a closed ValueReader" in {
    //      runThis(100.times) {
    //        assertSegment(
    //          keyValues = Slice(randomFixedKeyValue(1), randomFixedKeyValue(2)),
    //          assert =
    //            (keyValues, segment) =>
    //              keyValues foreach {
    //                keyValue =>
    //                  val readKeyValue = segment.get(keyValue.key).runIO
    //                  segment.close.runIO
    //                  readKeyValue.getOrFetchValue shouldBe keyValue.getOrFetchValue
    //              }
    //        )
    //      }
    //    }
    //
    //    "lazily load values" in {
    //      runThis(100.times) {
    //        assertSegment(
    //          keyValues = randomizedKeyValues(keyValuesCount),
    //          testAgainAfterAssert = false,
    //          assert =
    //            (keyValues, segment) =>
    //              keyValues foreach {
    //                keyValue =>
    //                  val readKeyValue = segment.get(keyValue.key).runIO
    //
    //                  readKeyValue match {
    //                    case persistent: Persistent.Remove =>
    //                      //remove has no value so isValueDefined will always return true
    //                      persistent.isValueDefined shouldBe true
    //
    //                    case persistent: Persistent =>
    //                      persistent.isValueDefined shouldBe false
    //
    //                    case _: Memory =>
    //                    //memory key-values always have values defined
    //                  }
    //                  //read the value
    //                  readKeyValue match {
    //                    case range: KeyValue.Range =>
    //                      range.fetchFromAndRangeValue.runIO
    //                    case _ =>
    //                      readKeyValue.getOrFetchValue shouldBe keyValue.getOrFetchValue
    //                  }
    //
    //                  //value is now set
    //                  readKeyValue match {
    //                    case persistent: Persistent =>
    //                      persistent.isValueDefined shouldBe true
    //
    //                    case _: Memory =>
    //                    //memory key-values always have values defined
    //                  }
    //              }
    //        )
    //      }
    //    }
  }
}
