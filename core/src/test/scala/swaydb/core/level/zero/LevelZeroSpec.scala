/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.level.zero

import org.scalamock.scalatest.MockFactory
import swaydb.IO
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.data.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.Memory
import swaydb.core.io.file.Effect
import swaydb.core.segment.ThreadReadState
import swaydb.core.{TestBase, TestCaseSweeper, TestForceSave, TestTimer}
import TestCaseSweeper._
import swaydb.data.compaction.Throttle
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice._
import swaydb.data.util.OperatingSystem
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._
import scala.util.Random
import swaydb.data.slice.Slice
import swaydb.data.slice.Slice.Sliced
import swaydb.data.util.ByteOps._

class LevelZeroSpec0 extends LevelZeroSpec

class LevelZeroSpec1 extends LevelZeroSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Enabled(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.Enabled(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.Enabled(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class LevelZeroSpec2 extends LevelZeroSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Disabled(forceSave = TestForceSave.channel())
  override def level0MMAP = MMAP.Disabled(forceSave = TestForceSave.channel())
  override def appendixStorageMMAP = MMAP.Disabled(forceSave = TestForceSave.channel())
}

class LevelZeroSpec3 extends LevelZeroSpec {
  override def inMemoryStorage = true
}

sealed trait LevelZeroSpec extends TestBase with MockFactory {

  implicit val keyOrder: KeyOrder[Sliced[Byte]] = KeyOrder.default
  implicit val testTimer: TestTimer = TestTimer.Empty
  implicit val timeOrder = TimeOrder.long

  import swaydb.core.map.serializer.LevelZeroMapEntryWriter._

  val keyValuesCount = 10

  //    override def deleteFiles = false

  "LevelZero" should {
    "initialise" in {
      TestCaseSweeper {
        implicit sweeper =>
          val nextLevel = TestLevel()
          val zero = TestLevelZero(Some(nextLevel))
          if (persistent) {
            zero.existsOnDisk shouldBe true
            nextLevel.existsOnDisk shouldBe true
            //maps folder is initialised
            Effect.exists(zero.path.resolve("0/0.log")) shouldBe true
            zero.reopen.existsOnDisk shouldBe true
          } else {
            zero.existsOnDisk shouldBe false
            nextLevel.existsOnDisk shouldBe false
          }
      }
    }
  }

  "LevelZero.put" should {
    "write key-value" in {
      TestCaseSweeper {
        implicit sweeper =>
          def assert(zero: LevelZero): Unit = {
            zero.put(1, "one").runRandomIO.value
            zero.get(1, ThreadReadState.random).getPut.getOrFetchValue shouldBe ("one": Sliced[Byte])

            zero.put("2", "two").runRandomIO.value
            zero.get("2", ThreadReadState.random).getPut.getOrFetchValue shouldBe ("two": Sliced[Byte])
          }

          val zero = TestLevelZero(Some(TestLevel(throttle = (_) => Throttle(10.seconds, 0))))
          assert(zero)
          if (persistent) assert(zero.reopen)
      }
    }

    "write key-values that have empty bytes but the Slices are closed" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel(throttle = (_) => Throttle(10.seconds, 0))
          val zero = TestLevelZero(Some(level))
          val one = Slice.create[Byte](10).addInt(1).close()

          zero.put(one, one).runRandomIO

          val gotFromLevelZero = zero.get(one, ThreadReadState.random).getPut.getOrFetchValue.getC
          gotFromLevelZero shouldBe one
          //ensure that key-values are not unsliced in LevelZero.
          gotFromLevelZero.underlyingArraySize shouldBe 10

          //the following does not apply to in-memory Levels
          //in-memory key-values are slice of the whole Segment.
          if (persistent) {
            //put the same key-value to Level1 and expect the key-values to be sliced
            level.putKeyValuesTest(Slice(Memory.put(one, one))).runRandomIO
            val gotFromLevelOne = level.get(one, ThreadReadState.random).getPut
            gotFromLevelOne.getOrFetchValue shouldBe one
            //ensure that key-values are not unsliced in LevelOne.
            gotFromLevelOne.getOrFetchValue.getC.underlyingArraySize shouldBe 4
          }
      }
    }

    "not write empty key-value" in {
      TestCaseSweeper {
        implicit sweeper =>
          val zero = TestLevelZero(Some(TestLevel()))
          IO(zero.put(Slice.empty, Slice.empty)).left.value shouldBe a[IllegalArgumentException]
      }
    }

    "write empty values" in {
      TestCaseSweeper {
        implicit sweeper =>
          val zero = TestLevelZero(Some(TestLevel()))
          zero.put(1, Slice.empty).runRandomIO
          zero.get(1, ThreadReadState.random).getPut.getOrFetchValue shouldBe Slice.Null
      }
    }

    "write large keys and values and reopen the database and re-read key-values" in {
      //approx 2 mb key and values
      TestCaseSweeper {
        implicit sweeper =>
          val key1 = "a" + Random.nextString(750000): Sliced[Byte]
          val key2 = "b" + Random.nextString(750000): Sliced[Byte]

          val value1 = Random.nextString(750000): Sliced[Byte]
          val value2 = Random.nextString(750000): Sliced[Byte]

          def assertWrite(zero: LevelZero): Unit = {
            zero.put(key1, value1).runRandomIO
            zero.put(key2, value2).runRandomIO
          }

          def assertRead(zero: LevelZero): Unit = {
            zero.get(key1, ThreadReadState.random).getPut.getOrFetchValue shouldBe value1
            zero.get(key2, ThreadReadState.random).getPut.getOrFetchValue shouldBe value2
          }

          val zero = TestLevelZero(Some(TestLevel(throttle = _ => Throttle(10.seconds, 0))))

          assertWrite(zero)
          assertRead(zero)

          //allow compaction to do it's work
          sleep(2.seconds)
          if (persistent) assertRead(zero.reopen)
      }
    }

    "write keys only" in {
      TestCaseSweeper {
        implicit sweeper =>
          val zero = TestLevelZero(Some(TestLevel()))

          zero.put("one").runRandomIO
          zero.put("two").runRandomIO

          zero.get("one", ThreadReadState.random).getPut.getOrFetchValue.toOptionC shouldBe empty
          zero.get("two", ThreadReadState.random).getPut.getOrFetchValue.toOptionC shouldBe empty

          zero.contains("one", ThreadReadState.random).runRandomIO.right.value shouldBe true
          zero.contains("two", ThreadReadState.random).runRandomIO.right.value shouldBe true
          zero.contains("three", ThreadReadState.random).runRandomIO.right.value shouldBe false
      }
    }

    "batch write key-values" in {
      TestCaseSweeper {
        implicit sweeper =>
          val keyValues = randomIntKeyStringValues(keyValuesCount)

          val zero = TestLevelZero(Some(TestLevel()))
          zero.put(_ => keyValues.toMapEntry.get).runRandomIO

          assertGet(keyValues, zero)

          zero.keyValueCount.runRandomIO.right.value shouldBe keyValues.size
      }
    }

    //removed test - empty check are performed at the source where the MapEntry is created.
    //    "batch writing empty keys should fail" in {
    //      if (persistent) {
    //        val keyValues = Slice(Memory.put(Slice.empty, 1))
    //
    //        val zero = TestLevelZero(Some(TestLevel()))
    //        assertThrows[Exception] {
    //          zero.put(_ => keyValues.toMapEntry.value)
    //        }
    //      } else {
    //        //Currently this test does not apply for in-memory. Empty keys should NEVER be written.
    //        //Persistent batch writes check for empty keys but since in-memory is just a skipList in Level0, there is no
    //        //check. The design of MapEntry restricts this which should be fixed.
    //      }
    //    }
  }

  "LevelZero.remove" should {
    "remove key-values" in {
      TestCaseSweeper {
        implicit sweeper =>
          val zero = TestLevelZero(Some(TestLevel(throttle = (_) => Throttle(10.seconds, 0))), mapSize = 1.byte)
          val keyValues = randomIntKeyStringValues(keyValuesCount)
          keyValues foreach {
            keyValue =>
              zero.put(keyValue.key, keyValue.getOrFetchValue).runRandomIO
          }

          if (unexpiredPuts(keyValues).nonEmpty)
            zero.head(ThreadReadState.random).runRandomIO.get.toOptionPut shouldBe defined

          keyValues foreach {
            keyValue =>
              zero.remove(keyValue.key).runRandomIO
          }

          zero.head(ThreadReadState.random).toOptionPut shouldBe empty
          zero.last(ThreadReadState.random).toOptionPut shouldBe empty
      }
    }

    "batch remove key-values" in {
      TestCaseSweeper {
        implicit sweeper =>
          val keyValues = randomIntKeyStringValues(keyValuesCount)
          val zero = TestLevelZero(Some(TestLevel()))
          zero.put(_ => keyValues.toMapEntry.get).runRandomIO

          assertGet(keyValues, zero)

          val removeKeyValues = Slice(keyValues.map(keyValue => Memory.remove(keyValue.key)).toArray)
          zero.put(_ => removeKeyValues.toMapEntry.get).runRandomIO

          assertGetNone(keyValues, zero)
          zero.head(ThreadReadState.random).toOptionPut shouldBe empty
      }
    }
  }

  "LevelZero.clear" should {
    "a database with single key-value" in {
      TestCaseSweeper {
        implicit sweeper =>
          val zero = TestLevelZero(Some(TestLevel(throttle = (_) => Throttle(10.seconds, 0))), mapSize = 1.byte)
          val keyValues = randomIntKeyStringValues(1)
          keyValues foreach {
            keyValue =>
              zero.put(keyValue.key, keyValue.getOrFetchValue).runRandomIO
          }

          zero.keyValueCount shouldBe 1

          zero.clear(ThreadReadState.random).runRandomIO.get

          zero.head(ThreadReadState.random).toOptionPut shouldBe empty
          zero.last(ThreadReadState.random).toOptionPut shouldBe empty
      }
    }

    "remove all key-values" in {
      TestCaseSweeper {
        implicit sweeper =>
          val zero = TestLevelZero(Some(TestLevel(throttle = (_) => Throttle(10.seconds, 0))), mapSize = 1.byte)
          val keyValues = randomIntKeyStringValues(keyValuesCount)
          keyValues foreach {
            keyValue =>
              zero.put(keyValue.key, keyValue.getOrFetchValue).runRandomIO
          }

          zero.clear(ThreadReadState.random).runRandomIO.get

          zero.head(ThreadReadState.random).toOptionPut shouldBe empty
          zero.last(ThreadReadState.random).toOptionPut shouldBe empty
      }
    }
  }

  "LevelZero.head" should {
    "return the first key-value" in {
      //disable throttle
      TestCaseSweeper {
        implicit sweeper =>
          val zero = TestLevelZero(Some(TestLevel(throttle = (_) => Throttle(10.seconds, 0))), mapSize = 1.byte)

          zero.put(1, "one").runRandomIO.value
          zero.put(2, "two").runRandomIO.value
          zero.put(3, "three").runRandomIO.value
          zero.put(4, "four").runRandomIO.value
          zero.put(5, "five").runRandomIO.value

          val head = zero.head(ThreadReadState.random).getPut
          head.key shouldBe (1: Sliced[Byte])
          head.getOrFetchValue shouldBe ("one": Sliced[Byte])

          //remove 1
          zero.remove(1).runRandomIO
          println
          zero.head(ThreadReadState.random).getPut.getOrFetchValue shouldBe ("two": Sliced[Byte])

          zero.remove(2).runRandomIO
          zero.remove(3).runRandomIO
          zero.remove(4).runRandomIO

          zero.head(ThreadReadState.random).getPut.getOrFetchValue shouldBe ("five": Sliced[Byte])

          zero.remove(5).runRandomIO
          zero.head(ThreadReadState.random).toOptionPut shouldBe empty
          zero.last(ThreadReadState.random).toOptionPut shouldBe empty
      }
    }
  }

  "LevelZero.last" should {
    "return the last key-value" in {
      TestCaseSweeper {
        implicit sweeper =>
          val zero = TestLevelZero(Some(TestLevel()), mapSize = 1.byte)

          zero.put(1, "one").runRandomIO
          zero.put(2, "two").runRandomIO
          zero.put(3, "three").runRandomIO
          zero.put(4, "four").runRandomIO
          zero.put(5, "five").runRandomIO

          zero.last(ThreadReadState.random).getPut.getOrFetchValue shouldBe ("five": Sliced[Byte])

          //remove 5
          zero.remove(5).runRandomIO
          zero.last(ThreadReadState.random).getPut.getOrFetchValue shouldBe ("four": Sliced[Byte])

          zero.remove(2).runRandomIO
          zero.remove(3).runRandomIO
          zero.remove(4).runRandomIO

          zero.last(ThreadReadState.random).getPut.getOrFetchValue shouldBe ("one": Sliced[Byte])

          zero.remove(1).runRandomIO
          zero.last(ThreadReadState.random).toOptionPut shouldBe empty
          zero.head(ThreadReadState.random).toOptionPut shouldBe empty
      }
    }
  }

  "LevelZero.remove range" should {
    "not allow from key to be > than to key" in {
      TestCaseSweeper {
        implicit sweeper =>
          val zero = TestLevelZero(Some(TestLevel()), mapSize = 1.byte)
          IO(zero.remove(10, 1)).left.value.getMessage shouldBe "fromKey should be less than toKey."
          IO(zero.remove(2, 1)).left.value.getMessage shouldBe "fromKey should be less than toKey."
      }
    }
  }

  "LevelZero.update range" should {
    "not allow from key to be > than to key" in {
      TestCaseSweeper {
        implicit sweeper =>
          val zero = TestLevelZero(Some(TestLevel()), mapSize = 1.byte)
          IO(zero.update(10, 1, value = "value")).left.value.getMessage shouldBe "fromKey should be less than toKey."
          IO(zero.update(2, 1, value = "value")).left.value.getMessage shouldBe "fromKey should be less than toKey."
      }
    }
  }
}
