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
import swaydb.core._
import swaydb.core.data.Value.FromValue
import swaydb.data.RunThis._
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.OperatingSystem
import swaydb.serializers.Default._
import swaydb.serializers._

class LevelMergeSpec0 extends LevelMergeSpec

class LevelMergeSpec1 extends LevelMergeSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class LevelMergeSpec2 extends LevelMergeSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.channel())
  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.channel())
  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.channel())
}

class LevelMergeSpec3 extends LevelMergeSpec {
  override def inMemoryStorage = true
}

sealed trait LevelMergeSpec extends TestBase with MockFactory with PrivateMethodTester {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val testTimer: TestTimer = TestTimer.Empty
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val ec = TestExecutionContext.executionContext
  val keyValuesCount = 100

  "level is empty" should {
    "write put key-values" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            //create a Level
            val level = TestLevel()

            //create put key-values so they do not get cleared.
            val keyValues = randomPutKeyValues(startId = Some(0))
            val segment = TestSegment(keyValues)

            //level is empty
            level.isEmpty shouldBe true
            assertEmpty(keyValues, level)

            //reserve
            val reservation = level.reserve(segment).rightValue

            //merge
            val compactionResult = level.merge(segment, reservation).await
            compactionResult should have size 1

            //level is still empty because nothing is committed.
            level.isEmpty shouldBe true

            //original segment does not get deleted
            segment.existsOnDiskOrMemory shouldBe true

            //commit
            level.commit(compactionResult).get shouldBe unit

            level.isEmpty shouldBe false
            assertReads(keyValues, level)
        }
      }
    }

    "cleared update key-values" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val level = TestLevel()

            //create put key-values so they do not get cleared.
            val keyValues = Slice(randomUpdateKeyValue(1), randomRangeKeyValue(2, 10, FromValue.Null), randomFunctionKeyValue(10))
            val segment = TestSegment(keyValues)

            //level is empty
            level.isEmpty shouldBe true

            //reserve
            val reservation = level.reserve(segment).rightValue

            //merge
            val compactionResult = level.merge(segment, reservation).awaitInf
            compactionResult should have size 1

            //level is still empty because nothing is committed.
            level.isEmpty shouldBe true

            //original segment does not get deleted
            segment.existsOnDiskOrMemory shouldBe true

            //commit
            level.commit(compactionResult).get shouldBe unit

            level.isEmpty shouldBe true
        }
      }
    }
  }

  "level is non empty" should {
    "overwrite existing key-values" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val level = TestLevel()

            //create put key-values so they do not get cleared.
            val keyValues = randomPutKeyValues(startId = Some(0))
            val segment = TestSegment(keyValues)

            val reservation = level.reserve(segment).rightValue

            val segmentMergeResult = level.merge(segment, reservation).awaitInf
            level.commit(segmentMergeResult).get shouldBe unit

            assertGet(keyValues, level)

            val map = TestMap(keyValues)
            val mapMergeResult = level.merge(map, reservation).awaitInf
            level.commit(mapMergeResult).get shouldBe unit

            assertGet(keyValues, level)

        }
      }
    }

    "merge new key-values" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val level = TestLevel()

            /**
             * FIRST with 100 key-values
             */
            val keyValues1 = randomPutKeyValues(startId = Some(0), count = 100)
            val segment = TestSegment(keyValues1)

            val reservation1 = level.reserve(segment).rightValue

            val segmentMergeResult = level.merge(segment, reservation1).awaitInf
            level.commit(segmentMergeResult).get shouldBe unit

            assertGet(keyValues1, level)
            level.checkout(reservation1)

            val oldSegments = level.segments().toList //toList so that it's immutable
            oldSegments.foreach(_.existsOnDiskOrMemory shouldBe true)

            /**
             * SECOND with 200 key-values
             */
            val keyValues2 = randomPutKeyValues(startId = Some(0), count = 200)

            val map = TestMap(keyValues2)
            val reservation2 = level.reserve(map).rightValue

            val mapMergeResult = level.merge(map, reservation2).awaitInf
            level.commit(mapMergeResult).get shouldBe unit

            assertGet(keyValues2, level)

            oldSegments.foreach(_.existsOnDiskOrMemory shouldBe false)

        }
      }
    }

    "seconds merge clears all existing key-values" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val level = TestLevel()

            /**
             * FIRST with 100 key-values
             */
            val keyValues1 = randomPutKeyValues(startId = Some(0), count = 100)
            val segment = TestSegment(keyValues1)

            val reservation1 = level.reserve(segment).rightValue

            val segmentMergeResult = level.merge(segment, reservation1).awaitInf
            level.commit(segmentMergeResult).get shouldBe unit

            assertGet(keyValues1, level)
            level.checkout(reservation1)

            val oldSegments = level.segments().toList //toList so that it's immutable
            oldSegments.foreach(_.existsOnDiskOrMemory shouldBe true)

            /**
             * SECOND with 200 key-values
             */
            val map = TestMap(Slice(randomRemoveRange(0, Int.MaxValue)))
            val reservation2 = level.reserve(map).rightValue

            val mapMergeResult = level.merge(map, reservation2).awaitInf
            level.commit(mapMergeResult).get shouldBe unit

            level.isEmpty shouldBe true

            oldSegments.foreach(_.existsOnDiskOrMemory shouldBe false)
        }
      }
    }
  }
}
