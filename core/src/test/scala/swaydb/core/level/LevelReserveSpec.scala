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
import swaydb.EitherValues._
import swaydb.IO
import swaydb.IOValues._
import swaydb.core.TestData._
import swaydb.core._
import swaydb.core.segment.assigner.Assignable
import swaydb.core.util.AtomicRanges
import swaydb.data.RunThis._
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.OperatingSystem
import swaydb.serializers.Default._
import swaydb.serializers._

class LevelReserveSpec0 extends LevelReserveSpec

class LevelReserveSpec1 extends LevelReserveSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class LevelReserveSpec2 extends LevelReserveSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.channel())
  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.channel())
  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.channel())
}

class LevelReserveSpec3 extends LevelReserveSpec {
  override def inMemoryStorage = true
}

sealed trait LevelReserveSpec extends TestBase with MockFactory with PrivateMethodTester {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val testTimer: TestTimer = TestTimer.Empty
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val ec = TestExecutionContext.executionContext
  val keyValuesCount = 100

  //      override def deleteFiles: Boolean =
  //        false

  "reserve keys for compaction where Level is empty" when {
    "single Segment" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel()

          val keyValues = randomizedKeyValues(keyValuesCount)
          val segment = TestSegment(keyValues)

          val reservation = level.reserve(Seq(segment)).rightValue
          reservation shouldBe AtomicRanges.Key.write(keyValues.head.key, keyValues.last.maxKey)

          //cannot reserve again
          val promises =
            Seq(
              () => level.reserve(Seq(segment)).leftValue,
              () => level.reserve(TestMap(keyValues)).leftValue
            ).runThisRandomlyValue

          promises foreach (_.isCompleted shouldBe false)

          level.checkout(reservation).get shouldBe unit

          promises foreach (_.isCompleted shouldBe true)
      }
    }

    "multiple Segments" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel()

          val keyValues = randomizedKeyValues(keyValuesCount)
          val groupKeyValues = keyValues.groupedSlice(2)
          val segment = TestSegment(groupKeyValues.head)
          val map = Assignable.Collection.fromMap(TestMap(groupKeyValues.last))

          val reservation = level.reserve(Seq(segment, map)).rightValue
          reservation shouldBe AtomicRanges.Key.write(keyValues.head.key, keyValues.last.maxKey)

          level.hasReservation(reservation) shouldBe true

          //cannot reserve again
          val promise1 = level.reserve(Seq(segment, map)).leftValue
          val promise2 = level.reserve(Seq(segment)).leftValue
          val promise3 = level.reserve(Seq(map)).leftValue

          val promises = Seq(promise1, promise2, promise3)
          promises foreach (_.isCompleted shouldBe false)

          level.checkout(reservation).get shouldBe unit

          promises foreach (_.isCompleted shouldBe true)
      }
    }

    "handle ranges" when {
      "head is fixed and tail is range" in {
        TestCaseSweeper {
          implicit sweeper =>
            val level = TestLevel()

            //       10, [11 - 20]
            val keyValues = Slice(randomFixedKeyValue(10), randomRangeKeyValue(15, 20))
            val segment = TestSegment(keyValues)

            val reservation = level.reserve(Seq(segment)).rightValue

            reservation.fromKey shouldBe 10.toBytes
            reservation.toKey shouldBe 20.toBytes
            reservation.toKeyInclusive shouldBe false

            //10 to 19
            val promise1 = level.reserve(TestMap(Slice(randomFixedKeyValue(10)))).get.leftValue
            val promise2 = level.reserve(TestMap(Slice(randomFixedKeyValue(11)))).get.leftValue
            val promise3 = level.reserve(TestSegment(Slice(randomFixedKeyValue(15)))).get.leftValue
            val promise4 = level.reserve(TestMap(Slice(randomFixedKeyValue(19)))).get.leftValue

            //failed to reserve
            val promises = Seq(promise1, promise2, promise3, promise4)
            promises.foreach(_.isCompleted shouldBe false)

            //reserve key 20 passes
            val reservationToKey20 = level.reserve(TestMap(Slice(randomFixedKeyValue(20)))).get.rightValue
            //again
            val promise5 = level.reserve(TestSegment(Slice(randomFixedKeyValue(20)))).get.leftValue
            promise5.isCompleted shouldBe false

            //release original
            level.checkout(reservation) shouldBe IO.unit
            promises.foreach(_.isCompleted shouldBe true)

            //still waiting
            promise5.isCompleted shouldBe false
            //release 20
            level.checkout(reservationToKey20)
            //finally released
            promise5.isCompleted shouldBe true
        }
      }

      "head is range and tail is fixed" in {
        TestCaseSweeper {
          implicit sweeper =>
            val level = TestLevel()

            //       10 - 20, 30
            val keyValues = Slice(randomRangeKeyValue(10, 20), randomFixedKeyValue(30))
            val segment = TestSegment(keyValues)

            val reservation = level.reserve(Seq(segment)).rightValue

            reservation.fromKey shouldBe 10.toBytes
            reservation.toKey shouldBe 30.toBytes
            reservation.toKeyInclusive shouldBe true

            //10 to 40
            val promise1 = level.reserve(TestMap(Slice(randomFixedKeyValue(10)))).get.leftValue
            val promise2 = level.reserve(TestSegment(Slice(randomFixedKeyValue(11)))).get.leftValue
            val promise3 = level.reserve(TestMap(Slice(randomFixedKeyValue(15)))).get.leftValue
            val promise4 = level.reserve(TestSegment(Slice(randomFixedKeyValue(19)))).get.leftValue
            val promise5 = level.reserve(TestSegment(Slice(randomFixedKeyValue(20)))).get.leftValue
            val promise6 = level.reserve(TestMap(Slice(randomFixedKeyValue(21)))).get.leftValue
            val promise7 = level.reserve(TestMap(Slice(randomFixedKeyValue(25)))).get.leftValue
            val promise8 = level.reserve(TestMap(Slice(randomFixedKeyValue(30)))).get.leftValue
            val promise9 = level.reserve(TestSegment(Slice(randomRangeKeyValue(1, 11)))).get.leftValue
            val promise10 = level.reserve(TestMap(Slice(randomRangeKeyValue(10, 25)))).get.leftValue
            val promise11 = level.reserve(TestSegment(Slice(randomRangeKeyValue(10, 40)))).get.leftValue

            //failed to reserve
            val promises = Seq(promise1, promise2, promise3, promise4, promise5, promise6, promise7, promise8, promise9, promise10, promise11)
            promises.foreach(_.isCompleted shouldBe false)

            //reserve key 41 passes
            val reservationToKey41 = level.reserve(TestMap(Slice(randomFixedKeyValue(41)))).get.rightValue
            //again
            val promise12 = level.reserve(TestMap(Slice(randomFixedKeyValue(41)))).get.leftValue
            promise12.isCompleted shouldBe false

            //release original
            level.checkout(reservation) shouldBe IO.unit
            promises.foreach(_.isCompleted shouldBe true)

            //still waiting
            promise12.isCompleted shouldBe false
            //release 41
            level.checkout(reservationToKey41)
            //finally released
            promise12.isCompleted shouldBe true
        }
      }
    }
  }
}
