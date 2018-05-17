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

import scala.concurrent.duration._
import scala.util.Random

//@formatter:off
class LevelZeroGetSpec1 extends LevelZeroGetSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class LevelZeroGetSpec2 extends LevelZeroGetSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class LevelZeroGetSpec3 extends LevelZeroGetSpec {
  override def inMemoryStorage = true
}
//@formatter:on

class LevelZeroGetSpec extends TestBase with MockFactory with Benchmark {

  implicit val ordering: Ordering[Slice[Byte]] = KeyOrder.default

  "Level0" should {

    "get key-value" in {
      val level1 = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))
      val level0 = TestLevelZero(level1, mapSize = randomNextInt(100).bytes, brake = Accelerator.cruise)

      level0.put(1, 1).assertGet
      level0.get(1).assertGet should contain(1: Slice[Byte])

      level0.head.assertGet shouldBe ((1: Slice[Byte], Some(1: Slice[Byte])))
      level0.last.assertGet shouldBe ((1: Slice[Byte], Some(1: Slice[Byte])))
    }

    "get None for removed key-value" in {
      val level1 = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))
      val level0 = TestLevelZero(level1, mapSize = randomNextInt(100).bytes, brake = Accelerator.cruise)

      level0.put(1, 1).assertGet
      level0.remove(1).assertGet

      level0.get(1).assertGetOpt shouldBe empty

      level0.head.assertGetOpt shouldBe empty
      level0.last.assertGetOpt shouldBe empty
    }

    "get updated Range's value" in {
      val level1 = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))
      val level0 = TestLevelZero(level1, mapSize = randomNextInt(100).byte, brake = Accelerator.cruise)

      (1 to 20) foreach {
        i =>
          level0.put(i, i).assertGet
      }

      //update range 5-10
      level0.update(5, 10, Some("updated")).assertGet

      Random.shuffle(1 to 20) foreach {
        i =>
          if (i >= 5 && i <= 10) //5-10 should be updated
            level0.get(i).assertGet should contain("updated": Slice[Byte])
          else
            level0.get(i).assertGet should contain(i: Slice[Byte])
      }

      level0.head.assertGet shouldBe ((1: Slice[Byte], Some(1: Slice[Byte])))
      level0.last.assertGet shouldBe ((20: Slice[Byte], Some(20: Slice[Byte])))
    }

    "get None for removed Range" in {
      val level1 = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))
      val level0 = TestLevelZero(level1, mapSize = randomNextInt(100).bytes, brake = Accelerator.cruise)

      (1 to 20) foreach {
        i =>
          level0.put(i, i).assertGet
      }

      //remove range 5-10
      level0.remove(5, 10).assertGet

      Random.shuffle(1 to 20) foreach {
        i =>
          if (i >= 5 && i <= 10) //5-10 should be updated
            level0.get(i).assertGetOpt shouldBe empty
          else
            level0.get(i).assertGet should contain(i: Slice[Byte])
      }

      level0.head.assertGet shouldBe ((1: Slice[Byte], Some(1: Slice[Byte])))
      level0.last.assertGet shouldBe ((20: Slice[Byte], Some(20: Slice[Byte])))
    }

    "get key-values from multiple maps" in {
      val level1 = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))
      val level0 = TestLevelZero(level1, mapSize = randomNextInt(100).bytes, brake = Accelerator.cruise)

      (1 to 20) foreach {
        i =>
          level0.put(i, i).assertGet
      }
      //random shuffle read
      Random.shuffle(1 to 20) foreach {
        i =>
          level0.get(i).assertGet should contain(i: Slice[Byte])
      }

      //sequential read
      (1 to 20) foreach {
        i =>
          level0.get(i).assertGet should contain(i: Slice[Byte])
      }

      level0.head.assertGet shouldBe ((1: Slice[Byte], Some(1: Slice[Byte])))
      level0.last.assertGet shouldBe ((20: Slice[Byte], Some(20: Slice[Byte])))
    }

    "get key-values from multiple maps with update ranges for all key-values" in {
      val level1 = TestLevel()
      val level0 = TestLevelZero(level1, mapSize = randomNextInt(100).bytes, brake = Accelerator.cruise)

      (1 to 20) foreach {
        i =>
          level0.put(i, i).assertGet
      }
      (1 to 20) foreach {
        i =>
          level0.update(i, i + 1, Some(s"$i updated")).assertGet
      }
      //random shuffle read
      Random.shuffle(1 to 20) foreach {
        i =>
          level0.get(i).assertGet should contain(s"$i updated": Slice[Byte])
      }

      //sequential read
      (1 to 20) foreach {
        i =>
          level0.get(i).assertGet should contain(s"$i updated": Slice[Byte])
      }

      level0.head.assertGet shouldBe ((1: Slice[Byte], Some("1 updated": Slice[Byte])))
      level0.last.assertGet shouldBe ((20: Slice[Byte], Some("20 updated": Slice[Byte])))
    }

    "get key-values from multiple maps with remove ranges for all key-values" in {
      val level1 = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))
      val level0 = TestLevelZero(level1, mapSize = randomNextInt(100).bytes, brake = Accelerator.cruise)

      (1 to 20) foreach {
        i =>
          level0.put(i, i).assertGet
      }

      (1 to 20) foreach {
        i =>
          level0.remove(i, i + 1).assertGet
      }
      //random shuffle read
      Random.shuffle(1 to 20) foreach {
        i =>
          level0.get(i).assertGetOpt shouldBe empty
      }

      //sequential read
      (1 to 20) foreach {
        i =>
          level0.get(i).assertGetOpt shouldBe empty
      }

      level0.head.assertGetOpt shouldBe empty
      level0.last.assertGetOpt shouldBe empty
    }
  }
}