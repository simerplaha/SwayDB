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

import swaydb.Exception.InvalidLevelReservation
import swaydb.IO
import swaydb.IOValues.IOEitherImplicits
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.level.compaction.reception.LevelReceptionKeyValidator.IterableCollectionKeyValidator
import swaydb.core.util.AtomicRanges
import swaydb.core.{TestBase, TestCaseSweeper}
import swaydb.data.RunThis._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class LevelReceptionKeyValidatorSpec extends TestBase {

  implicit val keyOrder = KeyOrder.default

  /**
   * The following test-cases test multiple scenarios so the test names
   * indicate the state (keys reserved) of the reservations before the tests starts.
   *
   * [1] means a fixed key-value
   * [1 - 10] means a range key-value
   *
   * [1], [2] - means 2 Segments with 1 and 2 key-value in each respectively
   * [[1 - 10], [11], [12]] - means 3 Segments with first Segment being a range key-value and other 2 Segments are fixed.
   */

  "fixed" when {
    "no reservation key" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            implicit val reservations: AtomicRanges[Slice[Byte]] = AtomicRanges[Slice[Byte]]()

            //[1]
            val keyValues = Slice(randomFixedKeyValue(1))
            val segment = TestSegment(keyValues)

            //reserve the Segment
            val key = LevelReception.reserve(Seq(segment), Iterable.empty).rightValue
            reservations.remove(key)

            //key was removed so validation fails
            LevelReceptionKeyValidator.validateIO(segment, key)(IO.unit).left.get shouldBe a[InvalidLevelReservation]
        }
      }
    }

    "[1]" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            implicit val reservations: AtomicRanges[Slice[Byte]] = AtomicRanges[Slice[Byte]]()

            //[1]
            val keyValues = Slice(randomFixedKeyValue(1))
            val segment = TestSegment(keyValues)

            //reserve the Segment
            val key = LevelReception.reserve(Seq(segment), Iterable.empty).rightValue

            //validate the Segment passes
            LevelReceptionKeyValidator.validateIO(segment, key)(IO.unit) shouldBe IO.unit

            LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomFixedKeyValue(0))), key)(IO.unit).left.get shouldBe a[InvalidLevelReservation]
            LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomFixedKeyValue(2))), key)(IO.unit).left.get shouldBe a[InvalidLevelReservation]
        }
      }
    }

    "[1, 2, 3]" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            implicit val reservations: AtomicRanges[Slice[Byte]] = AtomicRanges[Slice[Byte]]()

            //[1, 2, 3]
            val keyValues = Slice(randomFixedKeyValue(1), randomFixedKeyValue(2), randomFixedKeyValue(3))
            val segment = TestSegment(keyValues)

            //reserve the Segment
            val key = LevelReception.reserve(Seq(segment), Iterable.empty).rightValue
            //validate the Segment and it passes
            LevelReceptionKeyValidator.validateIO(segment, key)(IO.unit) shouldBe IO.unit

            //Slice the segment into 3 chunks
            val segmentsWithOneKeyValue = keyValues.groupedSlice(3).map(TestSegment(_))
            segmentsWithOneKeyValue should have size 3
            //each Segment has 1 key-value
            segmentsWithOneKeyValue.foreach(_.keyValueCount shouldBe 1)
            //validating each segment individually passes
            segmentsWithOneKeyValue foreach {
              segment =>
                LevelReceptionKeyValidator.validateIO(segment, key)(IO.unit) shouldBe IO.unit
            }

            //[0] fails
            LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomFixedKeyValue(0))), key)(IO.unit).left.get shouldBe a[InvalidLevelReservation]
            //[4] fails
            LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomFixedKeyValue(4))), key)(IO.unit).left.get shouldBe a[InvalidLevelReservation]

        }
      }
    }

    "[1], [2], [3]" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            implicit val reservations: AtomicRanges[Slice[Byte]] = AtomicRanges[Slice[Byte]]()

            //[1, 2, 3]
            val keyValues = Slice(Slice(randomFixedKeyValue(1)), Slice(randomFixedKeyValue(2)), Slice(randomFixedKeyValue(3)))
            val segments = keyValues.map(TestSegment(_))
            segments should have size 3

            val key = LevelReception.reserve(segments, Iterable.empty).rightValue

            //validate all segment together
            LevelReceptionKeyValidator.validateIO(segments, key)(IO.unit) shouldBe IO.unit
            //validate all Segments one by one
            segments.foreach(segment => LevelReceptionKeyValidator.validateIO(segment, key)(IO.unit) shouldBe IO.unit)

            //[0] fails
            LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomFixedKeyValue(0))), key)(IO.unit).left.get shouldBe a[InvalidLevelReservation]
            //[4] fails
            LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomFixedKeyValue(4))), key)(IO.unit).left.get shouldBe a[InvalidLevelReservation]

        }
      }
    }
  }

  "range" when {
    "no reservation key" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            implicit val reservations: AtomicRanges[Slice[Byte]] = AtomicRanges[Slice[Byte]]()

            //[10 - 20]
            val keyValues = Slice(randomRangeKeyValue(10, 20))
            val segment = TestSegment(keyValues)

            //reserve the Segment
            val key = LevelReception.reserve(Seq(segment), Iterable.empty).rightValue
            reservations.remove(key)

            //validation fails because key was removed.
            LevelReceptionKeyValidator.validateIO(segment, key)(IO.unit).left.get shouldBe a[InvalidLevelReservation]
        }
      }
    }

    "[10 - 20]" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            implicit val reservations: AtomicRanges[Slice[Byte]] = AtomicRanges[Slice[Byte]]()

            //[10 - 20]
            val keyValues = Slice(randomRangeKeyValue(10, 20))
            val segment = TestSegment(keyValues)

            //reserve the Segment
            val key = LevelReception.reserve(Seq(segment), Iterable.empty).rightValue

            //validate the Segment passes
            LevelReceptionKeyValidator.validateIO(segment, key)(IO.unit) shouldBe IO.unit

            LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomFixedKeyValue(9))), key)(IO.unit).left.get shouldBe a[InvalidLevelReservation]
            LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomFixedKeyValue(20))), key)(IO.unit).left.get shouldBe a[InvalidLevelReservation]
        }
      }
    }

    "[[10 - 20], [30 - 40], [50 - 60]]" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            implicit val reservations: AtomicRanges[Slice[Byte]] = AtomicRanges[Slice[Byte]]()

            val keyValues = Slice(Slice(randomRangeKeyValue(10, 20)), Slice(randomRangeKeyValue(30, 40)), Slice(randomRangeKeyValue(50, 60)))

            //create a single Segment or many
            val segments =
              eitherOne(
                Slice(TestSegment(keyValues.flatten)),
                keyValues.map(TestSegment(_))
              )

            //reserve the Segment
            val key = LevelReception.reserve(segments, Iterable.empty).rightValue

            //validate all Segments
            LevelReceptionKeyValidator.validateIO(segments, key)(IO.unit) shouldBe IO.unit
            //validate individual Segments
            segments.foreach(segment => LevelReceptionKeyValidator.validateIO(segment, key)(IO.unit) shouldBe IO.unit)

            //validate Segments that are not reserved.
            LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomFixedKeyValue(9))), key)(IO.unit).left.get shouldBe a[InvalidLevelReservation]
            LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomFixedKeyValue(60))), key)(IO.unit).left.get shouldBe a[InvalidLevelReservation]
            LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomRangeKeyValue(9, 60))), key)(IO.unit).left.get shouldBe a[InvalidLevelReservation]
        }
      }
    }

    "overlap logic check" when {

      /**
       * Checks overlapping logic in LevelReceptionKeyValidator.validateOrNull function
       */

      "RANGE [10 - 20]" in {
        runThis(10.times, log = true) {
          TestCaseSweeper {
            implicit sweeper =>

              implicit val reservations: AtomicRanges[Slice[Byte]] = AtomicRanges[Slice[Byte]]()

              //[10   -   20]
              val segment = TestSegment(Slice(randomRangeKeyValue(10, 20)))
              //reserve the Segment
              val key = LevelReception.reserve(Seq(segment), Iterable.empty).rightValue

              /**
               * RANGE
               */
              //PASSES
              //[10   -   20]
              LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomRangeKeyValue(10, 20))), key)(IO.unit) shouldBe IO.unit
              //[10  -  19]
              LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomRangeKeyValue(10, 19))), key)(IO.unit) shouldBe IO.unit
              //  [11 - 19]
              LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomRangeKeyValue(11, 19))), key)(IO.unit) shouldBe IO.unit

              //FAILS
              //[9   -   20]
              LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomRangeKeyValue(9, 20))), key)(IO.unit).left.get shouldBe a[InvalidLevelReservation]
              //  [10  -   21]
              LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomRangeKeyValue(10, 21))), key)(IO.unit).left.get shouldBe a[InvalidLevelReservation]
              //[9    -    21]
              LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomRangeKeyValue(9, 21))), key)(IO.unit).left.get shouldBe a[InvalidLevelReservation]

              /**
               * FIXED
               */
              //PASSES
              //10 to 19
              (10 to 19) foreach {
                int =>
                  LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomFixedKeyValue(int))), key)(IO.unit) shouldBe IO.unit
              }

              //FAILS
              //[0 - 9] and [20 - 30] are not reserved.
              ((0 to 9) ++ (20 to 30)) foreach {
                int =>
                  val keyValue =
                    eitherOne(
                      randomFixedKeyValue(int),
                      randomRangeKeyValue(int, int + 1)
                    )

                  LevelReceptionKeyValidator.validateIO(TestSegment(Slice(keyValue)), key)(IO.unit).left.get shouldBe a[InvalidLevelReservation]
              }


          }
        }
      }

      "RANGE & FIXED [[10 - 20], 21]" in {
        runThis(10.times, log = true) {
          TestCaseSweeper {
            implicit sweeper =>

              implicit val reservations: AtomicRanges[Slice[Byte]] = AtomicRanges[Slice[Byte]]()

              //[10   -   20], 21
              val segment = TestSegment(Slice(randomRangeKeyValue(10, 20), randomFixedKeyValue(21)))
              //reserve the Segment
              val key = LevelReception.reserve(Seq(segment), Iterable.empty).rightValue

              /**
               * RANGE
               */
              //PASSES
              //[10   -   21]
              LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomRangeKeyValue(10, 21))), key)(IO.unit) shouldBe IO.unit
              //[10  -  20]
              LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomRangeKeyValue(10, 20))), key)(IO.unit) shouldBe IO.unit
              //  [11  -  21]
              LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomRangeKeyValue(11, 21))), key)(IO.unit) shouldBe IO.unit
              //  [11 - 15], 21
              LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomRangeKeyValue(11, 15), randomFixedKeyValue(21))), key)(IO.unit) shouldBe IO.unit
              //[10 - 15], [15 - 21]
              LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomRangeKeyValue(10, 15), randomRangeKeyValue(15, 21))), key)(IO.unit) shouldBe IO.unit

              //FAILS
              //[10   -   22]
              LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomRangeKeyValue(9, 22))), key)(IO.unit).left.get shouldBe a[InvalidLevelReservation]
              //[  11   -   22]
              LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomRangeKeyValue(11, 22))), key)(IO.unit).left.get shouldBe a[InvalidLevelReservation]
              //  [10  -   22]
              LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomRangeKeyValue(10, 22))), key)(IO.unit).left.get shouldBe a[InvalidLevelReservation]
              //[9    -    22]
              LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomRangeKeyValue(9, 22))), key)(IO.unit).left.get shouldBe a[InvalidLevelReservation]
              //[10    -    21], 22
              LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomRangeKeyValue(10, 21), randomFixedKeyValue(22))), key)(IO.unit).left.get shouldBe a[InvalidLevelReservation]
              //[10 - 15], [15 - 22]
              LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomRangeKeyValue(10, 15), randomRangeKeyValue(15, 22))), key)(IO.unit).left.get shouldBe a[InvalidLevelReservation]

              /**
               * FIXED
               */
              //PASSES
              //10 to 21
              (10 to 21) foreach {
                int =>
                  LevelReceptionKeyValidator.validateIO(TestSegment(Slice(randomFixedKeyValue(int))), key)(IO.unit) shouldBe IO.unit
              }

              //FAILS
              //[0 - 9] and [22 - 30] are not reserved.
              ((0 to 9) ++ (22 to 30)) foreach {
                int =>
                  val keyValue =
                    eitherOne(
                      randomFixedKeyValue(int),
                      randomRangeKeyValue(int, int + 1)
                    )

                  LevelReceptionKeyValidator.validateIO(TestSegment(Slice(keyValue)), key)(IO.unit).left.get shouldBe a[InvalidLevelReservation]
              }
          }
        }
      }
    }

  }
}
