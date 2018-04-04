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

import java.nio.file.{Files, NoSuchFileException}

import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.ScalaFutures
import swaydb.core.TestBase
import swaydb.core.data.Transient.Remove
import swaydb.core.data._
import swaydb.core.map.serializer.RangeValueSerializers
import swaydb.core.map.serializer.RangeValueSerializers._
import swaydb.core.segment.SegmentException.SegmentCorruptionException
import swaydb.core.segment.format.one.SegmentWriter
import swaydb.data.segment.MaxKey.{Fixed, Range}
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

//@formatter:off
class SegmentGetSpec1 extends SegmentGetSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class SegmentGetSpec2 extends SegmentGetSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class SegmentGetSpec3 extends SegmentGetSpec {
  override def inMemoryStorage = true
}
//@formatter:on

class SegmentGetSpec extends TestBase with ScalaFutures with PrivateMethodTester {

  implicit val ordering = KeyOrder.default
  val keyValuesCount = 100

  "Segment.get" should {

    "return None key when the key is greater or less than Segment's min & maxKey and max Key is a Fixed key-value" in {
      assertOnSegment(
        keyValues = Slice(Memory.Put(1, 1)),
        assertion =
          segment => {
            segment.get(0).assertGetOpt shouldBe empty
            segment.get(2).assertGetOpt shouldBe empty
          }
      )

      assertOnSegment(
        keyValues = Slice(Memory.Put(1, 1), Memory.Put(2, 2)),
        assertion =
          segment => {
            segment.get(0).assertGetOpt shouldBe empty
            segment.get(3).assertGetOpt shouldBe empty
          }
      )
    }

    "return None key when the key is greater or less than Segment's min & maxKey and max Key is a Raneg key-value" in {
      assertOnSegment(
        keyValues = Slice(Memory.Range(1, 10, None, Value.Put(10))),
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
            Memory.Range(1, 10, None, Value.Put(10)),
            Memory.Range(10, 20, None, Value.Put(20))
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

    "get a Remove key-value" in {
      assertOnSegment(
        keyValues = Slice(Memory.Remove(1)),
        assertion = _.get(1).assertGet shouldBe Memory.Remove(1)
      )
    }

    "get Range key-values" in {
      assertOnSegment(
        keyValues = Slice(Memory.Range(1, 10, None, Value.Remove)),
        assertion = _.get(1).assertGet shouldBe Memory.Range(1, 10, None, Value.Remove)
      )

      assertOnSegment(
        keyValues = Slice(Memory.Range(1, 10, Some(Value.Remove), Value.Remove)),
        assertion = _.get(1).assertGet shouldBe Memory.Range(1, 10, Some(Value.Remove), Value.Remove)
      )

      assertOnSegment(
        keyValues = Slice(Memory.Range(1, 10, Some(Value.Put(1)), Value.Remove)),
        assertion = _.get(1).assertGet shouldBe Memory.Range(1, 10, Some(Value.Put(1)), Value.Remove)
      )

      assertOnSegment(
        keyValues = Slice(Memory.Range(1, 10, None, Value.Put(10))),
        assertion = _.get(1).assertGet shouldBe Memory.Range(1, 10, None, Value.Put(10))
      )

      assertOnSegment(
        keyValues = Slice(Memory.Range(1, 10, Some(Value.Remove), Value.Put(10))),
        assertion = _.get(1).assertGet shouldBe Memory.Range(1, 10, Some(Value.Remove), Value.Put(10))
      )

      assertOnSegment(
        keyValues = Slice(Memory.Range(1, 10, Some(Value.Put(1)), Value.Put(10))),
        assertion = _.get(1).assertGet shouldBe Memory.Range(1, 10, Some(Value.Put(1)), Value.Put(10))
      )
    }

    "get random key-values" in {
      val keyValues = randomIntKeyValues(keyValuesCount, addRandomDeletes = true, addRandomRanges = true)
      val segment = TestSegment(keyValues).assertGet
      assertGet(keyValues, segment)
    }

    "add unsliced key-values to Segment's caches" in {
      val keyValues = randomIntKeyStringValues(keyValuesCount, addRandomDeletes = true, addRandomRanges = true)
      val segment = TestSegment(keyValues).assertGet

      (0 until keyValues.size) foreach {
        index =>
          val keyValue = keyValues(index)
          if (persistent) segment.getFromCache(keyValue.key) shouldBe empty
          segment.get(keyValue.key).assertGet shouldBe keyValue

          val gotFromCache = eventually(segment.getFromCache(keyValue.key).assertGet)
          //underlying array sizes should not be slices but copies of arrays when the Segment is persistent
          if (persistent) {
            gotFromCache.key.underlyingArraySize shouldBe keyValue.key.toArray.length
            gotFromCache.getOrFetchValue.assertGetOpt.map(_.underlyingArraySize) shouldBe keyValue.getOrFetchValue.assertGetOpt.map(_.toArray.length)
          }
      }
    }

    "add read key values to cache" in {
      val keyValues = randomIntKeyValues(count = 10)
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
      val keyValues = randomIntKeyStringValues(keyValuesCount)
      val segment = TestSegment(keyValues).assertGet

      keyValues foreach {
        keyValue =>
          val readKeyValue = segment.get(keyValue.key).assertGet

          readKeyValue match {
            case persistent: Persistent =>
              persistent.isValueDefined shouldBe false
            case _ =>
          }
          //read the value
          readKeyValue.getOrFetchValue.assertGetOpt shouldBe keyValue.getOrFetchValue.assertGetOpt
          //value is now set
          readKeyValue match {
            case persistent: Persistent =>
              persistent.isValueDefined shouldBe true
            case _ =>
          }
      }
    }
  }
}
