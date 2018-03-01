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

package swaydb.core.level.zero

import org.scalamock.scalatest.MockFactory
import swaydb.core.TestBase
import swaydb.core.data.Transient
import swaydb.core.data.Transient.Remove
import swaydb.core.io.file.IO
import swaydb.core.util.Benchmark
import swaydb.data.compaction.Throttle
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._
import scala.util.Random

//@formatter:off
class LevelZeroSpec1 extends LevelZeroSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class LevelZeroSpec2 extends LevelZeroSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class LevelZeroSpec3 extends LevelZeroSpec {
  override def inMemoryStorage = true
}
//@formatter:on

class LevelZeroSpec extends TestBase with MockFactory with Benchmark {

  implicit val ordering: Ordering[Slice[Byte]] = KeyOrder.default

  import swaydb.core.map.serializer.LevelZeroMapEntryWriter._

  val keyValuesCount = 10

  //  override def deleteFiles = false

  "LevelZero" should {
    "initialise" in {
      val nextLevel = TestLevel()
      val zero = TestLevelZero(nextLevel)
      if (persistent) {
        zero.existsOnDisk shouldBe true
        nextLevel.existsOnDisk shouldBe true
        //maps folder is initialised
        IO.exists(zero.path.resolve("0/0.log")) shouldBe true
        zero.reopen.existsOnDisk shouldBe true
      }
    }
  }

  "LevelZero.put" should {
    "write key-value" in {
      def assert(zero: LevelZero): Unit = {
        zero.put(1, "one").assertGet
        zero.get(1).assertGet.assertGet shouldBe ("one": Slice[Byte])

        zero.put("2", "two").assertGet
        zero.get("2").assertGet.assertGet shouldBe ("two": Slice[Byte])
      }

      val zero = TestLevelZero(TestLevel(throttle = (_) => Throttle(10.seconds, 0)))
      assert(zero)
      if (persistent) assert(zero.reopen)

    }

    "write key-values that have empty bytes but the Slices are closed" in {
      val level = TestLevel(throttle = (_) => Throttle(10.seconds, 0))
      val zero = TestLevelZero(level)
      val one = Slice.create[Byte](10).addInt(1).close()

      zero.put(one, one).assertGet

      val gotFromLevelZero = zero.get(one).assertGet.assertGet
      gotFromLevelZero shouldBe one
      //ensure that key-values are not unsliced in LevelZero.
      gotFromLevelZero.underlyingArraySize shouldBe 10

      //the following does not apply to in-memory Levels
      //in-memory key-values are slice of the whole Segment.
      if (persistent) {
        //put the same key-value to Level1 and expect the key-values to be sliced
        level.putKeyValues(Slice(Transient.Put(one, one))).assertGet
        val gotFromLevelOne = level.get(one).assertGet
        gotFromLevelOne.getOrFetchValue.assertGet shouldBe one
        //ensure that key-values are not unsliced in LevelOne.
        gotFromLevelOne.getOrFetchValue.assertGet.underlyingArraySize shouldBe 4
      }
    }

    "not write empty key-value" in {
      val zero = TestLevelZero(TestLevel())
      zero.put(Slice.create[Byte](0), Slice.create[Byte](0)).failed.assertGet shouldBe a[IllegalArgumentException]
    }

    "write empty values" in {
      val zero = TestLevelZero(TestLevel())
      zero.put(1, Slice.create[Byte](0)).assertGet
      zero.get(1).assertGet.assertGet shouldBe Slice.create[Byte](0)
    }

    "write large keys and values and reopen the database and re-read key-values" in {
      //approx 2 mb key and values

      val key1 = "a" + Random.nextString(750000): Slice[Byte]
      val key2 = "b" + Random.nextString(750000): Slice[Byte]

      val value1 = Random.nextString(750000): Slice[Byte]
      val value2 = Random.nextString(750000): Slice[Byte]

      def assertWrite(zero: LevelZero): Unit = {
        zero.put(key1, value1).assertGet
        zero.put(key2, value2).assertGet
      }

      def assertRead(zero: LevelZero): Unit = {
        zero.get(key1).assertGet.assertGet shouldBe value1
        zero.get(key2).assertGet.assertGet shouldBe value2
      }

      val zero = TestLevelZero(TestLevel(throttle = _ => Throttle(10.seconds, 0)))
      assertWrite(zero)
      assertRead(zero)

      //allow compaction to do it's work
      sleep(2.seconds)
      if (persistent) assertRead(zero.reopen)
    }

    "write keys only" in {
      val zero = TestLevelZero(TestLevel())

      zero.put("one").assertGet
      zero.put("two").assertGet

      zero.get("one").assertGet shouldBe None
      zero.get("two").assertGet shouldBe None

      zero.contains("one").assertGet shouldBe true
      zero.contains("two").assertGet shouldBe true
      zero.contains("three").assertGet shouldBe false
    }

    "batch write key-values" in {
      val keyValues = randomIntKeyStringValues(keyValuesCount)

      val zero = TestLevelZero(TestLevel())
      zero.put(keyValues.toMapEntry.get).assertGet

      assertGet(keyValues, zero)
      assertHeadLast(keyValues, zero)

      zero.keyValueCount.assertGet shouldBe keyValues.size
    }

    "batch writing empty keys should fail" in {
      if (persistent) {
        val keyValues = Slice(Transient.Put(Slice.create[Byte](0), 1))

        val zero = TestLevelZero(TestLevel())
        assertThrows[Exception] {
          zero.put(keyValues.toMapEntry.get)
        }
      } else {
        //Currently this test does not apply for in-memory. Empty keys should NEVER be written.
        //Persistent batch writes check for empty keys but since in-memory is just a skipList in Level0, there is no
        //check. The design of MapEntry restricts this which should be fixed.
      }
    }
  }

  "LevelZero.remove" should {
    "remove key-values" in {
      val zero = TestLevelZero(TestLevel(throttle = (_) => Throttle(10.seconds, 0)), mapSize = 1.byte)
      val keyValues = randomIntKeyStringValues(keyValuesCount)
      keyValues foreach {
        keyValue =>
          zero.put(keyValue.key, keyValue.getOrFetchValue.assertGetOpt).assertGet
      }

      assertGet(keyValues, zero)

      keyValues foreach {
        keyValue =>
          zero.remove(keyValue.key).assertGet
      }

      zero.head.assertGetOpt shouldBe empty
      zero.last.assertGetOpt shouldBe empty
    }

    "batch remove key-values" in {
      val keyValues = randomIntKeyStringValues(keyValuesCount)
      val zero = TestLevelZero(TestLevel())
      zero.put(keyValues.toMapEntry.get).assertGet

      assertGet(keyValues, zero)

      val removeKeyValues = Slice(keyValues.map(keyValue => Remove(keyValue.key)).toArray)
      zero.put(removeKeyValues.toMapEntry.get).assertGet

      assertGetNone(keyValues, zero)
      zero.head.assertGetOpt shouldBe empty
    }
  }

  "LevelZero.head" should {
    "return the first key-value" in {
      //disable throttle
      val zero = TestLevelZero(TestLevel(throttle = (_) => Throttle(10.seconds, 0)), mapSize = 1.byte)

      zero.put(1, "one").assertGet
      zero.put(2, "two").assertGet
      zero.put(3, "three").assertGet
      zero.put(4, "four").assertGet
      zero.put(5, "five").assertGet

      val (headKey, headValue) = zero.head.assertGet
      headKey shouldBe (1: Slice[Byte])
      headValue.assertGet shouldBe ("one": Slice[Byte])

      //remove 1
      zero.remove(1).assertGet
      println
      val (headKey2, headValue2) = zero.head.assertGet
      println("headKey2: " + headKey2.read[Int])
      headKey2 shouldBe (2: Slice[Byte])
      headValue2.assertGet shouldBe ("two": Slice[Byte])

      zero.remove(2).assertGet
      zero.remove(3).assertGet
      zero.remove(4).assertGet

      println
      val (headKey5, headValue5) = zero.head.assertGet
      println("headKey5: " + headKey5.read[Int])
      headKey5 shouldBe (5: Slice[Byte])
      headValue5.assertGet shouldBe ("five": Slice[Byte])

      zero.remove(5).assertGet
      zero.head.assertGetOpt shouldBe empty
      zero.last.assertGetOpt shouldBe empty
    }
  }

  "LevelZero.last" should {
    "return the last key-value" in {
      val zero = TestLevelZero(TestLevel(), mapSize = 1.byte)

      zero.put(1, "one").assertGet
      zero.put(2, "two").assertGet
      zero.put(3, "three").assertGet
      zero.put(4, "four").assertGet
      zero.put(5, "five").assertGet

      val (lastKey, lastValue) = zero.last.assertGet
      lastKey shouldBe (5: Slice[Byte])
      lastValue.assertGet shouldBe ("five": Slice[Byte])

      //remove 5
      zero.remove(5).assertGet
      println
      val (lastKey4, lastValue4) = zero.last.assertGet
      println("lastKey4: " + lastKey4.read[Int])
      lastKey4 shouldBe (4: Slice[Byte])
      lastValue4.assertGet shouldBe ("four": Slice[Byte])

      zero.remove(2).assertGet
      zero.remove(3).assertGet
      zero.remove(4).assertGet

      println
      val (lastKey1, lastValue1) = zero.last.assertGet
      println("lastKey1: " + lastKey1.read[Int])
      lastKey1 shouldBe (1: Slice[Byte])
      lastValue1.assertGet shouldBe ("one": Slice[Byte])

      zero.remove(1).assertGet
      zero.last.assertGetOpt shouldBe empty
      zero.head.assertGetOpt shouldBe empty
    }
  }

  "LevelZero.sizeOfSegments" should {
    "return the size of Segments in all the levels" in {
      val one = TestLevel()
      val zero = TestLevelZero(one, mapSize = 100.byte)

      val keyValues = randomIntKeyStringValues(keyValuesCount)
      keyValues foreach {
        keyValue =>
          zero.put(keyValue.key, keyValue.getOrFetchValue.assertGetOpt).assertGet
      }
      eventual {
        zero.sizeOfSegments should be > 1L
      }
    }
  }
}
