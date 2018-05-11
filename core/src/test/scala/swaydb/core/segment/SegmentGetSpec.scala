/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment

import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.ScalaFutures
import swaydb.core.TestBase
import swaydb.core.data._
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._

class SegmentGetSpec0 extends SegmentGetSpec {
  val keyValuesCount = 100
}
//@formatter:off
class SegmentGetSpec1 extends SegmentGetSpec {
  val keyValuesCount = 100
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class SegmentGetSpec2 extends SegmentGetSpec {
  val keyValuesCount = 100
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
//@formatter:on

trait SegmentGetSpec extends TestBase with ScalaFutures with PrivateMethodTester {

  implicit val ordering = KeyOrder.default
  val keyValuesCount: Int

  "Segment.get" should {

    "return None key when the key is greater or less than Segment's min & maxKey and max Key is a Fixed key-value" in {
      assertOnSegment(
        keyValues = Slice(Memory.Put(1, randomStringOption, randomDeadlineOption)),
        assertion =
          segment => {
            segment.get(0).assertGetOpt shouldBe empty
            segment.get(2).assertGetOpt shouldBe empty
          }
      )

      assertOnSegment(
        keyValues = Slice(Memory.Put(1, randomStringOption, randomDeadlineOption), Memory.Put(2, randomStringOption, randomDeadlineOption)),
        assertion =
          segment => {
            segment.get(0).assertGetOpt shouldBe empty
            segment.get(3).assertGetOpt shouldBe empty
          }
      )
    }

    "return None key when the key is greater or less than Segment's min & maxKey and max Key is a Range key-value" in {
      assertOnSegment(
        keyValues = Slice(Memory.Range(1, 10, randomFromValueOption(), randomRangeValue())),
        assertion =
          segment => {
            segment.get(0).assertGetOpt shouldBe empty
            segment.get(10).assertGetOpt shouldBe empty
            segment.get(11).assertGetOpt shouldBe empty
          }
      )

      assertOnSegment(
        keyValues =
          Slice(
            Memory.Range(1, 10, randomFromValueOption(), randomRangeValue()),
            Memory.Range(10, 20, randomFromValueOption(), randomRangeValue())
          ),
        assertion =
          segment => {
            segment.get(0).assertGetOpt shouldBe empty
            segment.get(20).assertGetOpt shouldBe empty
            segment.get(21).assertGetOpt shouldBe empty
          }
      )
    }

    "get a Put key-value" in {
      assertOnSegment(
        keyValues = Slice(Memory.Put(1, 10)),
        assertion = _.get(1).assertGet shouldBe Memory.Put(1, 10)
      )
    }

    "get a Put key-value with deadline" in {
      val deadline = 2.second.fromNow
      assertOnSegment(
        keyValues = Slice(Memory.Put(1, 10, deadline)),
        assertion = _.get(1).assertGet shouldBe Memory.Put(1, 10, deadline)
      )
    }

    "get a Put key-value with expired deadline" in {
      val deadline = expiredDeadline()
      assertOnSegment(
        keyValues = Slice(Memory.Put(1, 10, deadline)),
        assertion = _.get(1).assertGet shouldBe Memory.Put(1, 10, deadline)
      )
    }

    "get a Remove key-value" in {
      assertOnSegment(
        keyValues = Slice(Memory.Remove(1)),
        assertion = _.get(1).assertGet shouldBe Memory.Remove(1)
      )
    }

    "get a Remove key-value with deadline" in {
      val deadline = 1.day.fromNow
      assertOnSegment(
        keyValues = Slice(Memory.Remove(1, deadline)),
        assertion = _.get(1).assertGet shouldBe Memory.Remove(1, deadline)
      )
    }

    "get a Remove key-value with expired" in {
      val deadline = expiredDeadline()
      assertOnSegment(
        keyValues = Slice(Memory.Remove(1, deadline)),
        assertion = _.get(1).assertGet shouldBe Memory.Remove(1, deadline)
      )
    }

    "get Range key-values" in {
      //run this test randomly to possibly test all range key-value combinations
      runThis(100.times) {
        val keyValue = randomRangeKeyValue(1, 10)
        assertOnSegment(
          keyValues = Slice(keyValue),
          assertion =
            level =>
              (1 to 9) foreach {
                i =>
                  level.get(i).assertGet shouldBe keyValue
                  level.get(10).assertGetOpt shouldBe empty
              }
        )
      }
    }

    "get random key-values" in {
      val keyValues = randomizedIntKeyValues(keyValuesCount)
      val segment = TestSegment(keyValues).assertGet
      assertGet(keyValues, segment)
    }

    "add unsliced key-values to Segment's caches" in {
      val keyValues = randomIntKeyValues(keyValuesCount, addRandomRemoves = true, addRandomRanges = true, addRandomPutDeadlines = true, addRandomRemoveDeadlines = true)
      val segment = TestSegment(keyValues).assertGet

      (0 until keyValues.size) foreach {
        index =>
          val keyValue = keyValues(index)
          if (persistent) segment.getFromCache(keyValue.key) shouldBe empty
          segment.get(keyValue.key).assertGet shouldBe keyValue

          val gotFromCache = eventually(segment.getFromCache(keyValue.key).assertGet)
          //underlying array sizes should not be slices but copies of arrays.
          gotFromCache.key.underlyingArraySize shouldBe keyValue.key.toArray.length

          gotFromCache match {
            case range: KeyValue.ReadOnly.Range =>
              //if it's a range, toKey should also be unsliced.
              range.toKey.underlyingArraySize shouldBe keyValues.find(_.key == range.fromKey).assertGet.key.toArray.length
            case _ =>
              gotFromCache.getOrFetchValue.assertGetOpt.map(_.underlyingArraySize) shouldBe keyValue.getOrFetchValue.assertGetOpt.map(_.toArray.length)

          }
      }
    }

    "add read key values to cache" in {
      val keyValues = randomIntKeyValues(keyValuesCount, addRandomRemoves = true, addRandomRanges = true, addRandomPutDeadlines = true, addRandomRemoveDeadlines = true)
      val segment = TestSegment(keyValues).assertGet

      keyValues foreach {
        keyValue =>
          if (persistent) segment isInCache keyValue.key shouldBe false
          (segment get keyValue.key).assertGet shouldBe keyValue
          eventually(segment isInCache keyValue.key shouldBe true)
      }
    }

    "read value from a closed ValueReader" in {
      val keyValues = Slice(Transient.Put(1, 1), Transient.Put(2, 2)).updateStats
      val segment = TestSegment(keyValues).assertGet

      val keyValue = segment.get(2).assertGet

      segment.close.assertGet

      keyValue.getOrFetchValue.assertGet shouldBe (2: Slice[Byte])

    }

    "lazily load values" in {
      val keyValues = randomIntKeyValues(keyValuesCount, addRandomRemoves = true, addRandomRanges = true, addRandomPutDeadlines = true, addRandomRemoveDeadlines = true)
      val segment = TestSegment(keyValues).assertGet

      keyValues foreach {
        keyValue =>
          val readKeyValue = segment.get(keyValue.key).assertGet

          readKeyValue match {
            case persistent: Persistent.Remove =>
              //remove has no value so isValueDefined will always return true
              persistent.isValueDefined shouldBe true

            case persistent: Persistent =>
              persistent.isValueDefined shouldBe false

            case _: Memory =>
            //memory key-values always have values defined
          }
          //read the value
          readKeyValue match {
            case range: KeyValue.ReadOnly.Range =>
              range.fetchFromAndRangeValue.assertGet
            case _ =>
              readKeyValue.getOrFetchValue.assertGetOpt shouldBe keyValue.getOrFetchValue.assertGetOpt
          }

          //value is now set
          readKeyValue match {
            case persistent: Persistent =>
              persistent.isValueDefined shouldBe true

            case _: Memory =>
            //memory key-values always have values defined
          }
      }
    }
  }
}