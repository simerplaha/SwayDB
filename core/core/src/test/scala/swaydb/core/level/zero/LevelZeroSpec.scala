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

package swaydb.core.level.zero

import org.scalamock.scalatest.MockFactory
import org.scalatest.OptionValues._
import swaydb.IO
import swaydb.IOValues._
import swaydb.config.{MMAP, TestForceSave}
import swaydb.config.compaction.LevelThrottle
import swaydb.config.storage.LevelStorage
import swaydb.core.CommonAssertions._
import swaydb.core.CorePrivateMethodTester._
import swaydb.core.CoreTestData._
import swaydb.core.log.applied.AppliedFunctionsLog
import swaydb.core.log.timer.Timer
import swaydb.core.segment.data.Memory
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.core.{ACoreSpec, TestSweeper, TestTimer}
import swaydb.core.level.ALevelSpec
import swaydb.effect.{Dir, Effect}
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.testkit.RunThis._
import swaydb.utils.OperatingSystem
import swaydb.utils.StorageUnits._

import scala.concurrent.duration._
import scala.util.Random
import swaydb.testkit.TestKit._
import swaydb.core.file.CoreFileTestKit._

class LevelZeroSpec0 extends LevelZeroSpec

class LevelZeroSpec1 extends LevelZeroSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
}

class LevelZeroSpec2 extends LevelZeroSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.standard())
  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.standard())
  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.standard())
}

class LevelZeroSpec3 extends LevelZeroSpec {
  override def isMemorySpec = true
}

sealed trait LevelZeroSpec extends ALevelSpec with MockFactory {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val testTimer: TestTimer = TestTimer.Empty
  implicit val timeOrder = TimeOrder.long

  import swaydb.core.log.serialiser.LevelZeroLogEntryWriter._

  val keyValuesCount = 10

  //    override def deleteFiles = false

  "LevelZero" should {
    "initialise" in {
      TestSweeper {
        implicit sweeper =>
          val nextLevel = TestLevel()
          val zero = TestLevelZero(Some(nextLevel))
          if (isPersistentSpec) {
            zero.existsOnDisk() shouldBe true
            nextLevel.existsOnDisk() shouldBe true
            //maps folder is initialised
            Effect.exists(zero.path.resolve("0/0.log")) shouldBe true
            zero.reopen.existsOnDisk() shouldBe true
          } else {
            zero.existsOnDisk() shouldBe false
            nextLevel.existsOnDisk() shouldBe false
          }
      }
    }
  }

  "LevelZero.put" should {
    "write key-value" in {
      TestSweeper {
        implicit sweeper =>
          def assert(zero: LevelZero): Unit = {
            zero.put(1, "one").runRandomIO.value
            zero.get(1, ThreadReadState.random).getPut.getOrFetchValue shouldBe ("one": Slice[Byte])

            zero.put("2", "two").runRandomIO.value
            zero.get("2", ThreadReadState.random).getPut.getOrFetchValue shouldBe ("two": Slice[Byte])
          }

          val zero = TestLevelZero(Some(TestLevel(throttle = (_) => LevelThrottle(10.seconds, 0))))
          assert(zero)
          if (isPersistentSpec) assert(zero.reopen)
      }
    }

    "write key-values that have empty bytes but the Slices are closed" in {
      TestSweeper {
        implicit sweeper =>
          import sweeper._

          val level = TestLevel(throttle = (_) => LevelThrottle(10.seconds, 0))
          val zero = TestLevelZero(Some(level))
          val one = Slice.allocate[Byte](10).addInt(1).close()

          zero.put(one, one).runRandomIO

          val gotFromLevelZero = zero.get(one, ThreadReadState.random).getPut.getOrFetchValue.getC
          gotFromLevelZero shouldBe one
          //ensure that key-values are not cutd in LevelZero.
          gotFromLevelZero.underlyingArraySize shouldBe 10

          //the following does not apply to in-memory Levels
          //in-memory key-values are slice of the whole Segment.
          if (isPersistentSpec) {
            //put the same key-value to Level1 and expect the key-values to be sliced
            level.put(Slice(Memory.put(one, one))).runRandomIO
            val gotFromLevelOne = level.get(one, ThreadReadState.random).getPut
            gotFromLevelOne.getOrFetchValue shouldBe one
            //ensure that key-values are not cutd in LevelOne.
            gotFromLevelOne.getOrFetchValue.getC.underlyingArraySize shouldBe 4
          }
      }
    }

    "not write empty key-value" in {
      TestSweeper {
        implicit sweeper =>
          val zero = TestLevelZero(Some(TestLevel()))
          assertThrows[IllegalArgumentException](zero.put(Slice.empty, Slice.empty))
      }
    }

    "write empty values" in {
      TestSweeper {
        implicit sweeper =>
          val zero = TestLevelZero(Some(TestLevel()))
          zero.put(1, Slice.empty).runRandomIO
          zero.get(1, ThreadReadState.random).getPut.getOrFetchValue shouldBe Slice.Null
      }
    }

    "write large keys and values and reopen the database and re-read key-values" in {
      //approx 2 mb key and values
      TestSweeper {
        implicit sweeper =>
          val key1 = "a" + Random.nextString(750000): Slice[Byte]
          val key2 = "b" + Random.nextString(750000): Slice[Byte]

          val value1 = Random.nextString(750000): Slice[Byte]
          val value2 = Random.nextString(750000): Slice[Byte]

          def assertWrite(zero: LevelZero): Unit = {
            zero.put(key1, value1).runRandomIO
            zero.put(key2, value2).runRandomIO
          }

          def assertRead(zero: LevelZero): Unit = {
            zero.get(key1, ThreadReadState.random).getPut.getOrFetchValue shouldBe value1
            zero.get(key2, ThreadReadState.random).getPut.getOrFetchValue shouldBe value2
          }

          val zero = TestLevelZero(Some(TestLevel(throttle = _ => LevelThrottle(10.seconds, 0))))

          assertWrite(zero)
          assertRead(zero)

          //allow compaction to do it's work
          sleep(2.seconds)
          if (isPersistentSpec) assertRead(zero.reopen)
      }
    }

    "write keys only" in {
      TestSweeper {
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
      TestSweeper {
        implicit sweeper =>
          val keyValues = randomIntKeyStringValues(keyValuesCount)

          val zero = TestLevelZero(Some(TestLevel()))
          zero.put(_ => keyValues.toLogEntry.get).runRandomIO

          assertGet(keyValues, zero)

          zero.keyValueCount.runRandomIO.right.value shouldBe keyValues.size
      }
    }

    //removed test - empty check are performed at the source where the LogEntry is created.
    //    "batch writing empty keys should fail" in {
    //      if (persistent) {
    //        val keyValues = Slice(Memory.put(Slice.empty, 1))
    //
    //        val zero = TestLevelZero(Some(TestLevel()))
    //        assertThrows[Exception] {
    //          zero.put(_ => keyValues.toLogEntry.value)
    //        }
    //      } else {
    //        //Currently this test does not apply for in-memory. Empty keys should NEVER be written.
    //        //Persistent batch writes check for empty keys but since in-memory is just a skipList in Level0, there is no
    //        //check. The design of LogEntry restricts this which should be fixed.
    //      }
    //    }
  }

  "LevelZero.remove" should {
    "remove key-values" in {
      TestSweeper {
        implicit sweeper =>
          val zero = TestLevelZero(Some(TestLevel(throttle = (_) => LevelThrottle(10.seconds, 0))), logSize = 1.byte)
          val keyValues = randomIntKeyStringValues(keyValuesCount, startId = Some(0))

          keyValues foreach {
            keyValue =>
              zero.put(keyValue.key, keyValue.getOrFetchValue).runRandomIO.get
          }

          if (unexpiredPuts(keyValues).nonEmpty)
            zero.head(ThreadReadState.random).runRandomIO.get.toOptionPut shouldBe defined

          keyValues foreach {
            keyValue =>
              zero.remove(keyValue.key).runRandomIO.get
          }

          zero.head(ThreadReadState.random).toOptionPut shouldBe empty
          zero.last(ThreadReadState.random).toOptionPut shouldBe empty
      }
    }

    "batch remove key-values" in {
      TestSweeper {
        implicit sweeper =>
          val keyValues = randomIntKeyStringValues(keyValuesCount)
          val zero = TestLevelZero(Some(TestLevel()))
          zero.put(_ => keyValues.toLogEntry.get).runRandomIO

          assertGet(keyValues, zero)

          val removeKeyValues = Slice.wrap(keyValues.mapToSlice(keyValue => Memory.remove(keyValue.key)).toArray)
          zero.put(_ => removeKeyValues.toLogEntry.get).runRandomIO

          assertGetNone(keyValues, zero)
          zero.head(ThreadReadState.random).toOptionPut shouldBe empty
      }
    }
  }

  "LevelZero.clear" should {
    "a database with single key-value" in {
      TestSweeper {
        implicit sweeper =>
          val zero = TestLevelZero(Some(TestLevel(throttle = (_) => LevelThrottle(10.seconds, 0))), logSize = 1.byte)
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
      runThis(10.times, log = true) {
        TestSweeper {
          implicit sweeper =>
            val zero = TestLevelZero(Some(TestLevel(throttle = (_) => LevelThrottle(10.seconds, 0))), logSize = 1.byte)
            val keyValues = randomIntKeyStringValues(randomIntMax(20) max keyValuesCount)

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
  }

  "LevelZero.head" should {
    "return the first key-value" in {
      //disable throttle
      TestSweeper {
        implicit sweeper =>
          val zero = TestLevelZero(Some(TestLevel(throttle = (_) => LevelThrottle(10.seconds, 0))), logSize = 1.byte)

          zero.put(1, "one").runRandomIO.value
          zero.put(2, "two").runRandomIO.value
          zero.put(3, "three").runRandomIO.value
          zero.put(4, "four").runRandomIO.value
          zero.put(5, "five").runRandomIO.value

          val head = zero.head(ThreadReadState.random).getPut
          head.key shouldBe (1: Slice[Byte])
          head.getOrFetchValue shouldBe ("one": Slice[Byte])

          //remove 1
          zero.remove(1).runRandomIO
          println
          zero.head(ThreadReadState.random).getPut.getOrFetchValue shouldBe ("two": Slice[Byte])

          zero.remove(2).runRandomIO
          zero.remove(3).runRandomIO
          zero.remove(4).runRandomIO

          zero.head(ThreadReadState.random).getPut.getOrFetchValue shouldBe ("five": Slice[Byte])

          zero.remove(5).runRandomIO
          zero.head(ThreadReadState.random).toOptionPut shouldBe empty
          zero.last(ThreadReadState.random).toOptionPut shouldBe empty
      }
    }
  }

  "LevelZero.last" should {
    "return the last key-value" in {
      TestSweeper {
        implicit sweeper =>
          val zero = TestLevelZero(Some(TestLevel()), logSize = 1.byte)

          zero.put(1, "one").runRandomIO
          zero.put(2, "two").runRandomIO
          zero.put(3, "three").runRandomIO
          zero.put(4, "four").runRandomIO
          zero.put(5, "five").runRandomIO

          zero.last(ThreadReadState.random).getPut.getOrFetchValue shouldBe ("five": Slice[Byte])

          //remove 5
          zero.remove(5).runRandomIO
          zero.last(ThreadReadState.random).getPut.getOrFetchValue shouldBe ("four": Slice[Byte])

          zero.remove(2).runRandomIO
          zero.remove(3).runRandomIO
          zero.remove(4).runRandomIO

          zero.last(ThreadReadState.random).getPut.getOrFetchValue shouldBe ("one": Slice[Byte])

          zero.remove(1).runRandomIO
          zero.last(ThreadReadState.random).toOptionPut shouldBe empty
          zero.head(ThreadReadState.random).toOptionPut shouldBe empty
      }
    }
  }

  "LevelZero.remove range" should {
    "not allow from key to be > than to key" in {
      TestSweeper {
        implicit sweeper =>
          val zero = TestLevelZero(Some(TestLevel()), logSize = 1.byte)
          IO(zero.remove(10, 1)).left.value.getMessage shouldBe "fromKey should be less than toKey."
          IO(zero.remove(2, 1)).left.value.getMessage shouldBe "fromKey should be less than toKey."
      }
    }
  }

  "LevelZero.update range" should {
    "not allow from key to be > than to key" in {
      TestSweeper {
        implicit sweeper =>
          val zero = TestLevelZero(Some(TestLevel()), logSize = 1.byte)
          IO(zero.update(10, 1, value = "value")).left.value.getMessage shouldBe "fromKey should be less than toKey."
          IO(zero.update(2, 1, value = "value")).left.value.getMessage shouldBe "fromKey should be less than toKey."
      }
    }
  }

  "timer and appliedFunctions" should {
    import Effect._

    "initiate" when {
      "timer is enabled" in {
        if (isPersistentSpec)
          TestSweeper {
            implicit sweeper =>
              val zero = TestLevelZero(None, enableTimer = true)
              zero.path.folderId shouldBe 0

              val timerPath = zero.path.getParent.resolve(Timer.folderName)
              Effect.exists(timerPath) shouldBe true

              //applied functions are also created
              val appliedFunctionsPath = zero.path.getParent.resolve(AppliedFunctionsLog.folderName)
              Effect.exists(appliedFunctionsPath) shouldBe true
          }
        else
          TestSweeper {
            implicit sweeper =>
              val zero = TestLevelZero(None, enableTimer = true)
              zero.path.folderId shouldBe 0
              getTimer(zero.logs).isEmptyTimer shouldBe false

              //applied functions are disabled for in-memory
              zero.appliedFunctionsLog shouldBe empty
          }
      }

      "timer is enabled for in-memory but next level is-persistent" in {
        if (isPersistentSpec)
          cancel("does not apply for in-memory")
        else
          TestSweeper {
            implicit sweeper =>

              val persistentLevel =
                TestLevel(
                  LevelStorage.Persistent(
                    dir = nextLevelPath,
                    otherDirs = eitherOne(Seq.empty, Seq(Dir(randomDir(), 1), Dir(randomDir(), 1))),
                    appendixMMAP = MMAP.randomForLog(),
                    appendixFlushCheckpointSize = 4.mb
                  )
                )

              val zero = TestLevelZero(Some(persistentLevel))

              //zero is in-memory but next level is persistent
              zero.inMemory shouldBe true
              persistentLevel.inMemory shouldBe false
              zero.nextLevel.value.rootPath shouldBe persistentLevel.rootPath

              persistentLevel.rootPath.folderId shouldBe a[Long]

              //persistent timer is still created
              val timerPath = persistentLevel.rootPath.getParent.resolve(Timer.folderName)
              Effect.exists(timerPath) shouldBe true

              //applied functions are also created
              val appliedFunctionsPath = persistentLevel.rootPath.getParent.resolve(AppliedFunctionsLog.folderName)
              Effect.exists(appliedFunctionsPath) shouldBe true
          }
      }
    }

    "not initiate" when {
      "timer is disabled" in {
        if (isPersistentSpec)
          TestSweeper {
            implicit sweeper =>
              val zero = TestLevelZero(None, enableTimer = false)
              import Effect._
              zero.path.folderId shouldBe 0

              val timerPath = zero.path.getParent.resolve(Timer.folderName)
              Effect.exists(timerPath) shouldBe false
          }
        else
          TestSweeper {
            implicit sweeper =>
              val zero = TestLevelZero(None, enableTimer = false)
              import Effect._
              zero.path.folderId shouldBe 0
              getTimer(zero.logs).isEmptyTimer shouldBe true
          }
      }
    }
  }
}
