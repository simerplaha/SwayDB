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
import swaydb.core.util.Benchmark
import swaydb.data.accelerate.Accelerator
import swaydb.data.compaction.Throttle
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.Random

//@formatter:off
class LevelZeroHigherSpec1 extends LevelZeroHigherSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class LevelZeroHigherSpec2 extends LevelZeroHigherSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class LevelZeroHigherSpec3 extends LevelZeroHigherSpec {
  override def inMemoryStorage = true
}
//@formatter:on

class LevelZeroHigherSpec extends TestBase with MockFactory with Benchmark {

  implicit val ordering: Ordering[Slice[Byte]] = KeyOrder.default

  "Level0.higher" should {

    "get None if the Levels are empty" in {
      val level1 = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))
      val level0 = TestLevelZero(level1, mapSize = randomNextInt(100).bytes, brake = Accelerator.cruise)

      (1 to 10) foreach {
        i =>
          level0.higher(i).assertGetOpt shouldBe empty
      }
    }

    "get higher key-value" in {
      val level1 = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))
      val level0 = TestLevelZero(level1, mapSize = randomNextInt(10).bytes, brake = Accelerator.cruise)

      (1 to 100) foreach {
        i =>
          level0.put(i, i).assertGet
      }

      Random.shuffle(1 to 99) foreach {
        i =>
          level0.higher(i).assertGetOpt should contain((i + 1: Slice[Byte], Some(i + 1: Slice[Byte])))
      }

    }

    "get higher key-value for updated key-value range" in {
      val level1 = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))
      val level0 = TestLevelZero(level1, mapSize = randomNextInt(10).bytes, brake = Accelerator.cruise)

      (1 to 100) foreach {
        i =>
          level0.put(i, i).assertGet
      }

      level0.update(1, 101, "updated").assertGet

      Random.shuffle(1 to 99) foreach {
        i =>
          level0.higher(i).assertGetOpt should contain((i + 1: Slice[Byte], Some("updated": Slice[Byte])))
      }

    }

    "get higher key-value for removed key-value range" in {
      val level1 = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))
      val level0 = TestLevelZero(level1, mapSize = randomNextInt(10).bytes, brake = Accelerator.cruise)

      (1 to 100) foreach {
        i =>
          level0.put(i, i).assertGet
      }

      level0.remove(10, 15).assertGet
      level0.remove(50, 60).assertGet
      level0.remove(18, 20).assertGet

      (1 to 99) foreach {
        i =>
          if (i >= 9 && i < 15)
            level0.higher(i).assertGetOpt should contain((15: Slice[Byte], Some(15: Slice[Byte])))
          else if (i >= 17 && i < 20)
            level0.higher(i).assertGetOpt should contain((20: Slice[Byte], Some(20: Slice[Byte])))
          else if (i >= 49 && i < 60)
            level0.higher(i).assertGetOpt should contain((60: Slice[Byte], Some(60: Slice[Byte])))
          else
            level0.higher(i).assertGetOpt should contain((i + 1: Slice[Byte], Some(i + 1: Slice[Byte])))
      }
    }
  }
}