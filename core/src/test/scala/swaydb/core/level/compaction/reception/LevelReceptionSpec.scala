/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.level.compaction.reception

import swaydb.IOValues.IOEitherImplicits
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.data.Memory
import swaydb.core.segment.Segment
import swaydb.core.segment.assigner.Assignable
import swaydb.core.util.AtomicRanges
import swaydb.core.{TestBase, TestCaseSweeper}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.testkit.RunThis._
import swaydb.{Error, IO}

import scala.concurrent.Promise

/**
 * Creates [[Segment]]s from key-values
 */
class Segment_LevelReceptionSpec extends LevelReceptionSpec {
  override def reserve(keyValues: Slice[Slice[Memory]],
                       levelSegments: Iterable[Segment])(implicit reservations: AtomicRanges[Slice[Byte]],
                                                         keyOrder: KeyOrder[Slice[Byte]],
                                                         testCaseSweeper: TestCaseSweeper): IO[Error.Level, Either[Promise[Unit], AtomicRanges.Key[Slice[Byte]]]] = {
    val segments = keyValues.map(keyValues => TestSegment(keyValues))
    LevelReception.reserve(segments, levelSegments)
  }
}

/**
 * Creates [[Map]]s from key-values
 */
class Map_LevelReceptionSpec extends LevelReceptionSpec {
  override def reserve(keyValues: Slice[Slice[Memory]],
                       levelSegments: Iterable[Segment])(implicit reservations: AtomicRanges[Slice[Byte]],
                                                         keyOrder: KeyOrder[Slice[Byte]],
                                                         testCaseSweeper: TestCaseSweeper): IO[Error.Level, Either[Promise[Unit], AtomicRanges.Key[Slice[Byte]]]] = {
    val segments = TestMap(keyValues.flatten)
    LevelReception.reserve(segments, levelSegments)
  }
}

/**
 * Creates [[Assignable.Collection]]s randomly from key-values
 */
class Collection_LevelReceptionSpec extends LevelReceptionSpec {
  override def reserve(keyValues: Slice[Slice[Memory]],
                       levelSegments: Iterable[Segment])(implicit reservations: AtomicRanges[Slice[Byte]],
                                                         keyOrder: KeyOrder[Slice[Byte]],
                                                         testCaseSweeper: TestCaseSweeper): IO[Error.Level, Either[Promise[Unit], AtomicRanges.Key[Slice[Byte]]]] = {
    val segments =
      keyValues map {
        keyValues =>
          //create either a Segment or a Map.
          eitherOne(
            TestSegment(keyValues),
            Assignable.Collection.fromMap(TestMap(keyValues))
          )
      }

    LevelReception.reserve(segments, levelSegments)
  }
}

sealed trait LevelReceptionSpec extends TestBase {

  implicit val keyOrder = KeyOrder.default

  def reserve(keyValues: Slice[Slice[Memory]],
              levelSegments: Iterable[Segment])(implicit reservations: AtomicRanges[Slice[Byte]],
                                                keyOrder: KeyOrder[Slice[Byte]],
                                                testCaseSweeper: TestCaseSweeper): IO[Error.Level, Either[Promise[Unit], AtomicRanges.Key[Slice[Byte]]]]

  "reserve" when {
    "fixed" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            implicit val ranges: AtomicRanges[Slice[Byte]] = AtomicRanges[Slice[Byte]]()

            //[1]
            val key1 = this.reserve(Slice(Slice(randomFixedKeyValue(1))), Iterable.empty).rightValue
            key1.fromKey shouldBe 1.serialise
            key1.toKey shouldBe 1.serialise
            key1.toKeyInclusive shouldBe true

            //[1] - fails
            val promise1 = this.reserve(Slice(Slice(randomFixedKeyValue(1))), Iterable.empty).leftValue
            promise1.isCompleted shouldBe false

            //[0, 1] - fails
            val promise2 = this.reserve(Slice(Slice(randomFixedKeyValue(0), randomFixedKeyValue(1))), Iterable.empty).leftValue
            promise2.isCompleted shouldBe false

            //[0], [1] - fails
            val promise3 = this.reserve(Slice(Slice(randomFixedKeyValue(0)), Slice(randomFixedKeyValue(1))), Iterable.empty).leftValue
            promise3.isCompleted shouldBe false

            //[0], [1], [2] - fails
            val promise4 = this.reserve(Slice(Slice(randomFixedKeyValue(0)), Slice(randomFixedKeyValue(1), randomFixedKeyValue(2))), Iterable.empty).leftValue
            promise4.isCompleted shouldBe false

            //[2] - passes
            val key2 = this.reserve(Slice(Slice(randomFixedKeyValue(2))), Iterable.empty).rightValue
            key2.fromKey shouldBe 2.serialise
            key2.toKey shouldBe 2.serialise
            key2.toKeyInclusive shouldBe true

            //[1] - fails still
            val promise5 = this.reserve(Slice(Slice(randomFixedKeyValue(1))), Iterable.empty).leftValue
            promise5.isCompleted shouldBe false

            ranges.remove(key1) shouldBe unit

            val promises = Seq(promise1, promise2, promise3, promise4, promise5)
            promises.foreach(_.isCompleted shouldBe true) //remove executes all promises

            //2 still cannot be reserved
            this.reserve(Slice(Slice(randomFixedKeyValue(2))), Iterable.empty).leftValue.isCompleted shouldBe false
        }
      }
    }

    "range" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            implicit val ranges: AtomicRanges[Slice[Byte]] = AtomicRanges[Slice[Byte]]()

            //[10 - 20]
            val key1 = this.reserve(Slice(Slice(randomRangeKeyValue(10, 20))), Iterable.empty).rightValue
            key1.fromKey shouldBe 10.serialise
            key1.toKey shouldBe 20.serialise
            key1.toKeyInclusive shouldBe false

            val promises =
              Seq(
                //[10] - fails
                () => this.reserve(Slice(Slice(randomFixedKeyValue(10))), Iterable.empty).leftValue,
                //[1 - 11] - fails
                () => this.reserve(Slice(Slice(randomRangeKeyValue(1, 11))), Iterable.empty).leftValue,
                //[15] - fails
                () => this.reserve(Slice(Slice(randomFixedKeyValue(15))), Iterable.empty).leftValue,
                //[10 - 19] - fails
                () => this.reserve(Slice(Slice(randomRangeKeyValue(10, 19))), Iterable.empty).leftValue,
                //[10 - 25] - fails
                () => this.reserve(Slice(Slice(randomRangeKeyValue(10, 25))), Iterable.empty).leftValue,
                //[0 - 25] - fails
                () => this.reserve(Slice(Slice(randomRangeKeyValue(0, 25))), Iterable.empty).leftValue
              ).runThisRandomlyValue

            promises.foreach(_.isCompleted shouldBe false)

            val passedKeys =
              Seq(
                //[9] - passes
                () => this.reserve(Slice(Slice(randomFixedKeyValue(9))), Iterable.empty).rightValue,
                //[20 - 30] || [20] - passes
                () => this.reserve(Slice(Slice(eitherOne(randomRangeKeyValue(20, 30), randomFixedKeyValue(20)))), Iterable.empty).rightValue,
              ).runThisRandomlyValue

            //promises are still incomplete
            promises.foreach(_.isCompleted shouldBe false)
            //remove key1
            ranges.remove(key1)
            //promises are now complete
            promises.foreach(_.isCompleted shouldBe true)

            //long range fails because passedKeys has overlapping keys
            val promiseLongRange = this.reserve(Slice(Slice(randomRangeKeyValue(0, 1000))), Iterable.empty).leftValue
            promiseLongRange.isCompleted shouldBe false
            //release passedKeys
            passedKeys.foreach(key => ranges.remove(key) shouldBe unit)
            promiseLongRange.isCompleted shouldBe true

        }
      }
    }
  }
}
