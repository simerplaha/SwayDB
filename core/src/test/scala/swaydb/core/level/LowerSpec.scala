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

package swaydb.core.level

import org.scalamock.scalatest.MockFactory
import swaydb.core.TestBase
import swaydb.core.data.{Memory, Persistent, Value}
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.util.Benchmark
import swaydb.core.util.FileUtil._
import swaydb.data.compaction.Throttle
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._

class LevelLowerSpec0 extends LevelLowerSpec

//@formatter:off
class LevelLowerSpec1 extends LevelLowerSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class LevelLowerSpec2 extends LevelLowerSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class LevelLowerSpec3 extends LevelLowerSpec {
  override def inMemoryStorage = true
}
//@formatter:on

sealed trait LevelLowerSpec extends TestBase with MockFactory with Benchmark {

  override implicit val ordering: Ordering[Slice[Byte]] = KeyOrder.default
  implicit override val groupingStrategy: Option[KeyValueGroupingStrategyInternal] = randomCompressionTypeOption(keyValuesCount)
  val keyValuesCount = 100

  "Level.lower" should {
    "return None if the Level is empty" in {
      val level = TestLevel()

      def doAssert(level: Level) =
        (1 to 10) foreach {
          key =>
            level.lower(key).assertGetOpt shouldBe empty
        }

      doAssert(level)
      doAssert(level) //again from cache

      if (persistent) {
        val levelReopened = level.reopen //reopen
        doAssert(levelReopened) //and read again from reopened
        doAssert(levelReopened) //again from cache
      }
    }

    "return None if the Level contains no lower" in {
      val keyValues = Slice(Memory.Put(11, "eleven"))
      val level = TestLevel()
      level.putKeyValues(keyValues).assertGet

      def doAssert(level: Level) =
        (1 to 10) foreach {
          key =>
            level.lower(key).assertGetOpt shouldBe empty
        }

      doAssert(level)
      doAssert(level) //again from cache

      if (persistent) {
        val levelReopened = level.reopen //reopen
        doAssert(levelReopened) //and read again from reopened
        doAssert(levelReopened) //again from cache
      }
    }

    "return None if the Level contains lower Remove" in {
      val keyValues = Slice(Memory.Remove(0))
      val level = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))
      level.putKeyValues(keyValues).assertGet

      def doAssert(level: Level) =
        (1 to 10) foreach {
          key =>
            level.isEmpty shouldBe false
            level.nextLevel.assertGet.isEmpty shouldBe true
            level.lower(key).assertGetOpt shouldBe empty
        }

      doAssert(level)
      doAssert(level) //again from cache

      if (persistent) {
        val levelReopened = level.reopen //reopen
        doAssert(levelReopened) //and read again from reopened
        doAssert(levelReopened) //again from cache
      }
    }

    "return None if upper Level and lower Level contains lower Remove and Remove range" in {
      val lowerLevel = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))
      val level = TestLevel(nextLevel = Some(lowerLevel), throttle = (_) => Throttle(Duration.Zero, 0))

      level.putKeyValues(Slice(Memory.Remove(1))).assertGet
      lowerLevel.putKeyValues(Slice(Memory.Remove(2), Memory.Range(5, 10, Some(Value.Remove(None)), Value.Remove(None)))).assertGet

      def doAssert(level: Level) =
        (10 to 21) foreach {
          key =>
            level.isEmpty shouldBe false
            level.nextLevel.assertGet.isEmpty shouldBe false
            level.lower(key).assertGetOpt shouldBe empty
        }

      doAssert(level)
      doAssert(level) //again from cache

      if (persistent) {
        val levelReopened = level.reopen //reopen
        doAssert(levelReopened) //and read again from reopened
        doAssert(levelReopened) //again from cache
      }
    }

    "return lower if the Level contains lower Put" in {
      val level = TestLevel()

      val keyValues = Slice(Memory.Put(0, "zero"))
      level.putKeyValues(keyValues).assertGet

      def doAssert(level: Level) = {
        //reopen and read again
        val lower = level.lower(1).assertGet
        if (persistent)
          lower should be(a[Persistent.Put])
        else
          lower should be(a[Memory.Put])

        lower shouldBe Memory.Put(0, Some("zero"))

        (1 to 10) foreach {
          key =>
            level.lower(key).assertGet shouldBe Memory.Put(0, Some("zero"))
        }
      }

      doAssert(level)
      doAssert(level) //again from cache

      if (persistent) {
        val levelReopened = level.reopen
        doAssert(levelReopened) //and read again from reopened
        doAssert(levelReopened) //again from cache
      }
    }

    "return None if the Level contains lower Remove range" in {
      //create a Level with lower level so that Range remove does not get deleted
      val level = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))

      val keyValues = Slice(Memory.Range(1, 10, None, Value.Remove(None)))
      level.putKeyValues(keyValues).assertGet
      level.segmentsCount() shouldBe 1

      def doAssert(level: Level) =
        (5 to 12) foreach {
          key =>
            level.lower(key).assertGetOpt shouldBe empty
        }

      doAssert(level)
      doAssert(level) //again from cache

      if (persistent) {
        val levelReopened = level.reopen //reopen
        doAssert(levelReopened) //and read again from reopened
        doAssert(levelReopened) //again from cache
      }
    }

    "return None if the Level contains lower Put range without any fixed Put keys in lower level" in {
      //create a Level with lower level so that Range remove does not get deleted
      val level = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))

      val keyValues = Slice(Memory.Range(1, 10, None, Value.Update(10)))
      level.putKeyValues(keyValues).assertGet
      level.segmentsCount() shouldBe 1

      def doAssert(level: Level) = {
        level.nextLevel.assertGet.isEmpty shouldBe true
        (0 to 15) foreach {
          key =>
            level.lower(key).assertGetOpt shouldBe empty
        }
      }

      doAssert(level)
      doAssert(level) //again from cache

      if (persistent) {
        val levelReopened = level.reopen //reopen
        doAssert(levelReopened) //and read again from reopened
        doAssert(levelReopened) //again from cache
      }
    }

    "return None if the Level contains lower Remove and Remove Ranges" in {
      val level = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))

      val keyValues =
        Slice(
          Memory.Remove(1),
          Memory.Range(2, 10, None, Value.Remove(None)),
          Memory.Range(10, 15, Some(Value.Remove(None)), Value.Remove(None))
        )

      level.putKeyValues(keyValues).assertGet

      def doAssert(level: Level) =
        (0 to 20) foreach {
          key =>
            level.lower(key).assertGetOpt shouldBe empty
        }

      doAssert(level)
      doAssert(level)

      if (persistent) {
        val reopenedLevel = level.reopen
        //reopen and read again
        doAssert(reopenedLevel)
        doAssert(reopenedLevel)
      }
    }

    "return None if the Level contains lower Put ranges" in {
      val level = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))

      val keyValues =
        Slice(
          Memory.Range(2, 10, None, Value.Update(10)),
          Memory.Range(10, 15, None, Value.Update(15)),
          Memory.Range(16, 20, None, Value.Update(20))
        )

      level.putKeyValues(keyValues).assertGet

      def doAssert(level: Level) =
        (0 to 21) foreach {
          key =>
            level.lower(key).assertGetOpt shouldBe empty
        }

      doAssert(level)
      doAssert(level)

      if (persistent) {
        val reopenedLevel = level.reopen
        //reopen and read again
        doAssert(reopenedLevel)
        doAssert(reopenedLevel)
      }
    }

    "return None if the Level contains lower Remove, Remove Ranges & Put Ranges" in {
      val level = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))

      val keyValues =
        Slice(
          Memory.Remove(1),
          Memory.Range(2, 10, None, Value.Remove(None)),
          Memory.Range(10, 20, Some(Value.Remove(None)), Value.Update(10)),
          Memory.Range(25, 30, None, Value.Remove(None)),
          Memory.Remove(30),
          Memory.Range(31, 35, None, Value.Update(30)),
          Memory.Range(40, 45, Some(Value.Remove(None)), Value.Remove(None))
        )

      level.putKeyValues(keyValues).assertGet

      def doAssert(level: Level) =
        (0 to 50) foreach {
          key =>
            level.isEmpty shouldBe false
            level.nextLevel.assertGet.isEmpty shouldBe true
            level.lower(key).assertGetOpt shouldBe empty
        }

      doAssert(level)
      doAssert(level)

      if (persistent) {
        val reopenedLevel = level.reopen
        //reopen and read again
        doAssert(reopenedLevel)
        doAssert(reopenedLevel)
      }
    }

    "return lower if the Level contains lower Put in upper level" in {
      val level = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))

      val keyValues =
        Slice(
          Memory.Put(1),
          Memory.Range(2, 10, None, Value.Remove(None)),
          Memory.Range(10, 20, Some(Value.Remove(None)), Value.Update(10)),
          Memory.Range(25, 30, None, Value.Remove(None)),
          Memory.Remove(30),
          Memory.Range(31, 35, None, Value.Update(30)),
          Memory.Range(40, 45, Some(Value.Remove(None)), Value.Remove(None)),
          Memory.Remove(100)
        )

      level.putKeyValues(keyValues).assertGet

      def doAssert(level: Level) =
        (2 to 50) foreach {
          key =>
            level.isEmpty shouldBe false
            level.nextLevel.assertGet.isEmpty shouldBe true
            level.lower(key).assertGet shouldBe Memory.Put(1, None)
        }

      doAssert(level)
      doAssert(level)

      if (persistent) {
        val reopenedLevel = level.reopen
        //reopen and read again
        doAssert(reopenedLevel)
        doAssert(reopenedLevel)
      }
    }

    "return lower if the Lower Level contains lower Put" in {
      val lowerLevel = TestLevel()
      val level = TestLevel(nextLevel = Some(lowerLevel), throttle = (_) => Throttle(Duration.Zero, 0))

      val keyValues =
        Slice(
          Memory.Remove(1),
          Memory.Range(2, 10, None, Value.Remove(None)),
          Memory.Range(10, 20, Some(Value.Remove(None)), Value.Update(10)),
          Memory.Range(25, 30, None, Value.Remove(None)),
          Memory.Remove(30),
          Memory.Range(31, 35, None, Value.Update(30)),
          Memory.Range(40, 45, Some(Value.Remove(None)), Value.Remove(None))
        )

      level.putKeyValues(keyValues).assertGet

      lowerLevel.putKeyValues(Slice(Memory.Put(0))).assertGet

      def doAssert(level: Level) =
        (1 to 50) foreach {
          key =>
            level.isEmpty shouldBe false
            level.nextLevel.assertGet.isEmpty shouldBe false
            level.lower(key).assertGet shouldBe Memory.Put(0, None)
        }

      doAssert(level)
      doAssert(level)

      if (persistent) {
        val reopenedLevel = level.reopen
        //reopen and read again
        doAssert(reopenedLevel)
        doAssert(reopenedLevel)
      }
    }

    "return lower if the Level contains Ranges and Removes and lower level contains key-values" in {

      val lowerLevel = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))
      val level = TestLevel(nextLevel = Some(lowerLevel), throttle = (_) => Throttle(Duration.Zero, 0))

      val keyValues =
        Slice(
          Memory.Remove(5),
          Memory.Range(7, 10, None, Value.Remove(None)),
          Memory.Range(10, 20, Some(Value.Remove(None)), Value.Update(10)),
          Memory.Range(25, 30, None, Value.Remove(None)),
          Memory.Remove(30),
          Memory.Range(31, 35, None, Value.Update(30)),
          Memory.Range(40, 45, Some(Value.Remove(None)), Value.Remove(None))
        )

      level.putKeyValues(keyValues).assertGet

      //add remove for each 100 key-values in lower level and all key-value should still return empty
      (1 to 100) foreach {
        key =>
          lowerLevel.putKeyValues(Slice(Memory.Remove(key - 1), Memory.Remove(key), Memory.Remove(key + 1))).assertGet
          lowerLevel.isEmpty shouldBe false
          level.lower(key).assertGetOpt shouldBe empty
      }

      lowerLevel.segmentsInLevel() should have size 1
      lowerLevel.segmentsInLevel().head.path.fileId.assertGet._1 shouldBe 100 //100 updates occurred

      //40 - 44  is remove in upper level, inserting put in lower level should still return empty
      (40 to 44) foreach {
        key =>
          lowerLevel.putKeyValues(Slice(Memory.Put(key))).assertGet
          level.lower(key).assertGetOpt shouldBe empty
      }

      //35 - 39 is empty, inserting put in lower level should return lower
      (35 to 39) foreach {
        key =>
          lowerLevel.putKeyValues(Slice(Memory.Put(key))).assertGet
          level.lower(key + 1).assertGet shouldBe Memory.Put(key)
      }

      //31 - 34 is Put(30), inserting put in lower level should return lower with put set from upper level.
      (31 to 34) foreach {
        key =>
          lowerLevel.putKeyValues(Slice(Memory.Put(key))).assertGet
          level.lower(key + 1).assertGet shouldBe Memory.Put(key, 30)
      }

      //25 - 30 is Remove, inserting Put for these key-value to lower Level will still return empty.
      (25 to 30) foreach {
        key =>
          lowerLevel.putKeyValues(Slice(Memory.Put(key))).assertGet
          level.lower(key + 1).assertGetOpt shouldBe empty
      }

      //upper Levels 10 is Remove.
      lowerLevel.putKeyValues(Slice(Memory.Put(10, 10))).assertGet
      level.lower(11).assertGetOpt shouldBe empty

      //11 - 19 is Put range, inserting Put for these key-value to lower Level will return lower with updated Put value
      (11 to 19) foreach {
        key =>
          lowerLevel.putKeyValues(Slice(Memory.Put(key))).assertGet
          level.lower(key + 1).assertGet shouldBe Memory.Put(key, 10)
      }

    }
  }
}
