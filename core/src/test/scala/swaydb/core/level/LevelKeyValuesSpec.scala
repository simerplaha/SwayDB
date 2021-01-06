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

package swaydb.core.level

import org.scalamock.scalatest.MockFactory
import org.scalatest.PrivateMethodTester
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.data._
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core._
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.data.RunThis._
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.OperatingSystem
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._

class LevelKeyValuesSpec0 extends LevelKeyValuesSpec

class LevelKeyValuesSpec1 extends LevelKeyValuesSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class LevelKeyValuesSpec2 extends LevelKeyValuesSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.channel())
  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.channel())
  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.channel())
}

class LevelKeyValuesSpec3 extends LevelKeyValuesSpec {
  override def inMemoryStorage = true
}

sealed trait LevelKeyValuesSpec extends TestBase with MockFactory with PrivateMethodTester {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val testTimer: TestTimer = TestTimer.Empty
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  val keyValuesCount = 100

  //  override def deleteFiles: Boolean =
  //    false

  "put KeyValues" should {
    "write a key-values to the Level" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel()

          val keyValues = randomPutKeyValues(startId = Some(1))
          level.put(keyValues).runRandomIO.right.value
          level.put(Slice(keyValues.head)).runRandomIO.right.value

          level.segments() foreach {
            segment =>
              segment.createdInLevel shouldBe level.levelNumber
          }

          assertReads(keyValues, level)

          if (persistent) {
            val reopen = level.reopen
            assertReads(keyValues, reopen)
          }
      }
    }

    "return an empty level if all the key values in the Level were REMOVED and if Level is the only Level" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, mmap = mmapSegments))

          val keyValues = randomPutKeyValues(keyValuesCount)
          level.put(keyValues).get.fromKey shouldBe keyValues.head.key

          val deleteKeyValues = Slice.of[Memory](keyValues.size * 2)
          keyValues foreach {
            keyValue =>
              deleteKeyValues add Memory.remove(keyValue.key)
          }
          //also add another set of Delete key-values where the keys do not belong to the Level but since there is no lower level
          //these delete keys should also be removed
          val lastKeyValuesId = keyValues.last.key.read[Int] + 10000

          (lastKeyValuesId until keyValues.size + lastKeyValuesId) foreach {
            id =>
              deleteKeyValues add Memory.remove(id, randomly(expiredDeadline()))
          }

          level.put(deleteKeyValues).get
          level.segmentFilesInAppendix shouldBe 0

          level.isEmpty shouldBe true
          if (persistent) {
            level.reopen.isEmpty shouldBe true
            level.segmentFilesOnDisk shouldBe empty
          }
      }
    }

    "not return an empty level if all the key values in the Level were REMOVED but it has lower level" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel(nextLevel = Some(TestLevel()))

          val keyValues = randomPutKeyValues()
          level.put(keyValues).runRandomIO.right.value

          val deleteKeyValues = Slice.of[Memory](keyValues.size)
          keyValues foreach {
            keyValue =>
              deleteKeyValues add Memory.remove(keyValue.key)
          }

          level.put(deleteKeyValues).runRandomIO.right.value
          level.isEmpty shouldBe false
          keyValues foreach {
            keyValue =>
              level.get(keyValue.key, ThreadReadState.random).runRandomIO.right.value.toOptionPut shouldBe empty
          }
      }
    }

    "return an empty level if all the key values in the Level were REMOVED by RANGE and if Level is the only Level" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, mmap = mmapSegments))

          val keyValues = randomPutKeyValues(keyValuesCount)
          level.put(keyValues).runRandomIO.right.value

          level.put(Slice(Memory.Range(keyValues.head.key, keyValues.last.key.readInt() + 1, Value.FromValue.Null, Value.remove(None)))).runRandomIO.right.value
          level.segmentFilesInAppendix shouldBe 0

          level.isEmpty shouldBe true
          if (persistent) {
            level.reopen.isEmpty shouldBe true
            level.segmentFilesOnDisk shouldBe empty
          }
      }
    }

    "not return an empty level if all the key values in the Level were REMOVED by RANGE but it has a lower Level" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, mmap = mmapSegments), nextLevel = Some(TestLevel()))

          val keyValues = randomPutKeyValues(keyValuesCount)
          level.put(keyValues).runRandomIO.right.value
          val segmentsCountBeforeRemove = level.segmentFilesInAppendix

          level.put(Slice(Memory.Range(keyValues.head.key, keyValues.last.key.readInt() + 1, Value.FromValue.Null, Value.remove(None)))).runRandomIO.right.value
          level.segmentFilesInAppendix shouldBe segmentsCountBeforeRemove

          level.isEmpty shouldBe false
          if (persistent) {
            level.reopen.isEmpty shouldBe false
            level.segmentFilesOnDisk should have size segmentsCountBeforeRemove
          }
      }
    }

    "return an empty level if all the key values in the Level were EXPIRED and if Level is the only Level" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, mmap = mmapSegments))

          val keyValues = randomPutKeyValues(keyValuesCount)
          level.put(keyValues).runRandomIO.right.value

          val deleteKeyValues = Slice.of[Memory](keyValues.size * 2)
          keyValues foreach {
            keyValue =>
              deleteKeyValues add Memory.remove(keyValue.key, 1.seconds)
          }
          //also add another set of Delete key-values where the keys do not belong to the Level but since there is no lower level
          //these delete keys should also be removed
          val lastKeyValuesId = keyValues.last.key.read[Int] + 1
          (lastKeyValuesId until keyValues.size + lastKeyValuesId) foreach {
            id =>
              deleteKeyValues add Memory.remove(id, randomly(expiredDeadline()))
          }

          level.nextLevel shouldBe empty

          level.put(deleteKeyValues).runRandomIO.right.value

          sleep(2.seconds)

          level.segments() foreach {
            segment =>
              level.commit(level.refresh(Seq(segment), level.reserve(segment).rightValue).get).get shouldBe unit
          }

          //expired key-values return empty after 2.seconds
          keyValues foreach {
            keyValue =>
              level.get(keyValue.key, ThreadReadState.random).runRandomIO.right.value.toOptionPut shouldBe empty
          }

          level.segmentFilesInAppendix shouldBe 0

          level.isEmpty shouldBe true

          if (persistent) {
            level.reopen.isEmpty shouldBe true
            level.segmentFilesOnDisk shouldBe empty
          }
      }
    }

    "not return an empty level if all the key values in the Level were EXPIRED and if Level has a lower Level" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, mmap = mmapSegments), nextLevel = Some(TestLevel()))

          val keyValues = randomPutKeyValues(keyValuesCount)
          level.put(keyValues).runRandomIO.right.value

          val deleteKeyValues = Slice.of[Memory](keyValues.size * 2)
          keyValues foreach {
            keyValue =>
              deleteKeyValues add Memory.remove(keyValue.key, 0.seconds)
          }
          //also add another set of Delete key-values where the keys do not belong to the Level but since there is no lower level
          //these delete keys should also be removed
          val lastKeyValuesId = keyValues.last.key.read[Int] + 1
          (lastKeyValuesId until keyValues.size + lastKeyValuesId) foreach {
            id =>
              deleteKeyValues add Memory.remove(id, randomly(expiredDeadline()))
          }

          level.put(deleteKeyValues).runRandomIO.right.value

          //expired key-values return empty.
          keyValues foreach {
            keyValue =>
              level.get(keyValue.key, ThreadReadState.random).runRandomIO.right.value.toOptionPut shouldBe empty
          }

          //sleep for 2.seconds and Segments should still exists.
          sleep(2.seconds)
          level.isEmpty shouldBe false
          level.segmentFilesInAppendix should be >= 1

          if (persistent) {
            level.reopen.isEmpty shouldBe false
            level.segmentFilesOnDisk.size should be >= 1
          }
      }
    }

    "return an empty level if all the key values in the Level were EXPIRED by RANGE and if Level is the only Level" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, mmap = mmapSegments))

          val keyValues = randomPutKeyValues(keyValuesCount)
          level.put(keyValues).runRandomIO.right.value

          level.put(Slice(Memory.Range(keyValues.head.key, keyValues.last.key.readInt() + 1, Value.FromValue.Null, Value.remove(2.seconds.fromNow)))).runRandomIO.right.value

          //expired key-values return empty after 2.seconds
          eventual(5.seconds) {
            keyValues foreach {
              keyValue =>
                level.get(keyValue.key, ThreadReadState.random).runRandomIO.right.value.toOptionPut shouldBe empty
            }
          }

          level.commit(level.refresh(level.segments(), level.reserve(level.segments()).rightValue).get).get shouldBe unit

          level.segmentFilesInAppendix shouldBe 0

          level.isEmpty shouldBe true

          if (persistent) {
            level.reopen.isEmpty shouldBe true
            level.segmentFilesOnDisk shouldBe empty
          }
      }
    }

    "not return an empty level if all the key values in the Level were EXPIRED by RANGE and if Level has a last Level" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, mmap = mmapSegments), nextLevel = Some(TestLevel()))

          val keyValues = randomPutKeyValues(keyValuesCount)
          level.put(keyValues).runRandomIO.right.value

          level.put(Slice(Memory.Range(keyValues.head.key, keyValues.last.key.readInt() + 1, Value.FromValue.Null, Value.remove(2.seconds.fromNow)))).runRandomIO.right.value

          //expired key-values return empty after 2.seconds
          eventual(5.seconds) {
            keyValues foreach {
              keyValue =>
                level.get(keyValue.key, ThreadReadState.random).runRandomIO.right.value.toOptionPut shouldBe empty
            }
          }

          level.segmentFilesInAppendix should be >= 1

          level.isEmpty shouldBe false

          if (persistent) {
            level.reopen.isEmpty shouldBe false
            level.segmentFilesOnDisk.size should be >= 1
          }
      }
    }
  }
}
