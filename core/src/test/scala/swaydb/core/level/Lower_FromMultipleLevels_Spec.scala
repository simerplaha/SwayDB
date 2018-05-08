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
import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.data.Memory
import swaydb.core.segment.KeyValueMerger
import swaydb.core.util.Benchmark
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._
import scala.util.Random

//@formatter:off
class Lower_FromMultipleLevels_Spec0 extends Lower_FromMultipleLevels_Spec {
  override val maxIterations: Int = 100
}

class Lower_FromMultipleLevels_Spec1 extends Lower_FromMultipleLevels_Spec {
  override val maxIterations: Int = 100

  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class Lower_FromMultipleLevels_Spec2 extends Lower_FromMultipleLevels_Spec {
  override val maxIterations: Int = 100

  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class Lower_FromMultipleLevels_Spec3 extends Lower_FromMultipleLevels_Spec {
  override val maxIterations: Int = 1000
  override def inMemoryStorage = true
}
//@formatter:on

trait Lower_FromMultipleLevels_Spec extends TestBase with MockFactory with Benchmark {

  implicit val ordering: Ordering[Slice[Byte]] = KeyOrder.default
  val maxIterations: Int

  "Lower" should {
    "empty Level" in {
      assertOnLevel(
        upperLevelKeyValues = Slice.empty,
        lowerLevelKeyValues = Slice.empty,
        assertion =
          level =>
            (1 to 10) foreach {
              i =>
                level.lower(i).assertGetOpt shouldBe empty
            }
      )
    }
  }

  //Converts the input Range key-value to Fixed else returns the input key-value.
  def toFixed(keyValue: Memory): Memory.Fixed =
    keyValue match {
      case range: Memory.Range =>
        range.fromValue.getOrElse(range.rangeValue).toMemory(range.fromKey)

      case keyValue: Memory.Fixed =>
        keyValue
    }

  "Lower" when {
    "Left" in {
      //10 - 20 (input keys)
      //10 - 20 (lower range)
      //run this test multiple times to execute on multiple possible combinations
      runThis(maxIterations.times) {
        //randomly generate a key-value where the key-values overlap.
        val upperLevelKeyValue = if (Random.nextBoolean()) randomRangeKeyValue(5, 10) else randomFixedKeyValue(5)
        val lowerLevelKeyValue = if (Random.nextBoolean()) randomRangeKeyValue(5, 10) else randomFixedKeyValue(5)

        //      val upperLevelKeyValue = Memory.Range(5, 10, Some(Value.Update(5, None)), Value.Remove(1000.seconds.fromNow))
        //      val lowerLevelKeyValue = Memory.Update(5, None, 2099.seconds.fromNow)

        //      val upperLevelKeyValue = Memory.Range(5, 10, Some(Value.Update(5, None)), Value.Remove(1000.seconds.fromNow))
        //      val lowerLevelKeyValue = Memory.Range(5, 10, Some(Value.Update(10, None)), Value.Update(None, None))

        println
        println("upperLevelKeyValue: " + upperLevelKeyValue)
        println("lowerLevelKeyValue: " + lowerLevelKeyValue)

        assertOnLevel(
          upperLevelKeyValues = Slice(upperLevelKeyValue),
          lowerLevelKeyValues = Slice(lowerLevelKeyValue),
          assertion =
            level =>
              (toFixed(upperLevelKeyValue), toFixed(lowerLevelKeyValue)) match {
                //If remove hasTimeLeft and lower Level's range has Put which is also not expired, then lower should return the Put's value.
                case (remove: Memory.Remove, put: Memory.Put) if remove.hasTimeLeft() && put.hasTimeLeft() =>
                  //expected Put will contain Remove's deadline if deadline exists else put's deadline.
                  val expected = remove.deadline.map(put.updateDeadline) getOrElse put
                  (6 to 15) foreach (i => level.lower(i).assertGet shouldBe expected)
                  (0 to 5) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                case (_: Memory.Remove, _) => //else remove has not time left. If from value is not Put all key-values should return empty since rangeValues are not Put.
                  (0 to 15) foreach { i => level.lower(i).assertGetOpt shouldBe empty }

                case (put: Memory.Put, _) =>
                  if (put.hasTimeLeft()) (6 to 15) foreach (i => level.lower(i).assertGet shouldBe put)
                  (0 to 5) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                case (update: Memory.Update, put: Memory.Put) if update.hasTimeLeft() && put.hasTimeLeft() =>
                  val expected =
                    if (update.deadline.isDefined)
                      update.toPut()
                    else
                      put.deadline.map(update.toPut) getOrElse update.toPut()

                  (6 to 15) foreach (i => level.lower(i).assertGet shouldBe expected)
                  (0 to 5) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                case (_: Memory.Update, _) =>
                  (1 to 15) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
              }
        )
      }
    }

    "Left - 2" in {
      //10 - 20 (input keys)
      //10 - 20 (lower range)
      //run this test multiple times to execute on multiple possible combinations
      runThis(maxIterations.times) {
        //randomly generate a key-value where the key-values overlap.
        //5   -   10 (lower range)
        //  6 -> 9   (lower possible keys from next)
        val upperLevelKeyValue = if (Random.nextBoolean()) randomRangeKeyValue(5, 10) else randomFixedKeyValue(5)
        val lowerLevelKeyValue = if (Random.nextBoolean()) randomRangeKeyValue(6, 9) else randomFixedKeyValue(6)

        //      val upperLevelKeyValue = Memory.Range(5, 10, Some(Value.Remove(None)), Value.Update(None, 1000.seconds))
        //      val lowerLevelKeyValue = Memory.Range(6, 9, Some(Value.Put(None, 100.seconds)), Value.Update(None, 200.seconds))

        //        val upperLevelKeyValue = Memory.Range(5, 10, Some(Value.Remove(1000.seconds.fromNow)), Value.Remove(None))
        //        //      val lowerLevelKeyValue = Memory.Range(6, 9, Some(Value.Put(None, 100.seconds)), Value.Update(None, 200.seconds))
        //        val lowerLevelKeyValue = Memory.Put(6, None, 10000.seconds.fromNow)

        println
        println("upperLevelKeyValue: " + upperLevelKeyValue)
        println("lowerLevelKeyValue: " + lowerLevelKeyValue)

        val expectedLowerValue: ReadOnly.Fixed =
          (upperLevelKeyValue, lowerLevelKeyValue) match {
            case (upperRange: Memory.Range, lowerRange: Memory.Range) =>
              KeyValueMerger.applyValue(upperRange.rangeValue.toMemory(6), lowerRange.fromValue.getOrElse(lowerRange.rangeValue).toMemory(6), Duration.Zero).assertGet

            case (upperRange: Memory.Range, lower: Memory.Fixed) =>
              KeyValueMerger.applyValue(upperRange.rangeValue.toMemory(6), lower, Duration.Zero).assertGet

            case (_: Memory.Fixed, lowerRange: Memory.Range) =>
              toFixed(lowerRange)

            case (_: Memory.Fixed, lower: Memory.Fixed) =>
              lower
          }

        assertOnLevel(
          upperLevelKeyValues = Slice(upperLevelKeyValue),
          lowerLevelKeyValues = Slice(lowerLevelKeyValue),
          assertion =
            level =>
              (toFixed(upperLevelKeyValue), expectedLowerValue) match {
                //If remove hasTimeLeft and lower Level's range has Put which is also not expired, then lower should return the Put's value.
                case (_: Memory.Remove, put: Memory.Put) =>
                  //expected Put will contain Remove's deadline if deadline exists else put's deadline.
                  if (put.hasTimeLeft()) {
                    (7 to 22) foreach { i => level.lower(i).assertGet shouldBe put }
                    (0 to 6) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  }
                  else
                    (0 to 5) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                case (_: Memory.Remove, _) =>
                  (0 to 15) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                case (upperPut: Memory.Put, lowerPut: Memory.Put) =>
                  if (upperPut.hasTimeLeft() && !lowerPut.hasTimeLeft()) {
                    (6 to 22) foreach (i => level.lower(i).assertGet shouldBe upperPut)
                    (0 to 5) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  } else if (!upperPut.hasTimeLeft() && lowerPut.hasTimeLeft()) {
                    (7 to 22) foreach (i => level.lower(i).assertGet shouldBe lowerPut)
                    (0 to 6) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  } else if (upperPut.hasTimeLeft() && lowerPut.hasTimeLeft()) {
                    (6 to 6) foreach (i => level.lower(i).assertGet shouldBe upperPut)
                    (7 to 22) foreach (i => level.lower(i).assertGet shouldBe lowerPut)
                    (0 to 5) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  } else {
                    (0 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  }

                case (upperPut: Memory.Put, _) =>
                  if (upperPut.hasTimeLeft())
                    (6 to 22) foreach (i => level.lower(i).assertGet shouldBe upperPut)
                  else
                    (6 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                  (0 to 5) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                case (_: Memory.Update, put: Memory.Put) =>
                  if (put.hasTimeLeft())
                    (7 to 22) foreach (i => level.lower(i).assertGet shouldBe put)
                  else
                    (7 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                  (0 to 6) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                case (_: Memory.Update, _) =>
                  (0 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
              }
        )
      }
    }

    "Left - 3" in {
      //       10 - 20
      // 5 -> 9
      runThis(maxIterations.times) {
        //randomly generate a key-value where the key-values overlap.
        //         10   -   20 (lower range)
        //  5 -> 9             (lower possible keys from next)
        val upperLevelKeyValue = if (Random.nextBoolean()) randomRangeKeyValue(10, 20) else randomFixedKeyValue(10)
        val lowerLevelKeyValue = if (Random.nextBoolean()) randomRangeKeyValue(5, 9) else randomFixedKeyValue(5)

        //      val upperLevelKeyValue = Memory.Range(5, 10, Some(Value.Remove(None)), Value.Update(None, 1000.seconds))
        //      val lowerLevelKeyValue = Memory.Range(6, 9, Some(Value.Put(None, 100.seconds)), Value.Update(None, 200.seconds))

        //        val upperLevelKeyValue = Memory.Range(5, 10, Some(Value.Remove(1000.seconds.fromNow)), Value.Remove(None))
        //        //      val lowerLevelKeyValue = Memory.Range(6, 9, Some(Value.Put(None, 100.seconds)), Value.Update(None, 200.seconds))
        //        val lowerLevelKeyValue = Memory.Put(6, None, 10000.seconds.fromNow)

        println
        println("upperLevelKeyValue: " + upperLevelKeyValue)
        println("lowerLevelKeyValue: " + lowerLevelKeyValue)

        assertOnLevel(
          upperLevelKeyValues = Slice(upperLevelKeyValue),
          lowerLevelKeyValues = Slice(lowerLevelKeyValue),
          assertion =
            level =>
              (toFixed(upperLevelKeyValue), toFixed(lowerLevelKeyValue)) match {
                //If remove hasTimeLeft and lower Level's range has Put which is also not expired, then lower should return the Put's value.
                case (_: Memory.Remove, put: Memory.Put) =>
                  //expected Put will contain Remove's deadline if deadline exists else put's deadline.
                  if (put.hasTimeLeft()) {
                    (6 to 22) foreach { i => level.lower(i).assertGet shouldBe put }
                    (0 to 5) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  } else
                    (0 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                case (_: Memory.Remove, _) =>
                  (0 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                case (upperPut: Memory.Put, lowerPut: Memory.Put) =>
                  if (upperPut.hasTimeLeft() && !lowerPut.hasTimeLeft()) {
                    (11 to 22) foreach (i => level.lower(i).assertGet shouldBe upperPut)
                    (0 to 10) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  } else if (!upperPut.hasTimeLeft() && lowerPut.hasTimeLeft()) {
                    (6 to 22) foreach (i => level.lower(i).assertGet shouldBe lowerPut)
                    (0 to 5) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  } else if (upperPut.hasTimeLeft() && lowerPut.hasTimeLeft()) {
                    (11 to 22) foreach (i => level.lower(i).assertGet shouldBe upperPut)
                    (6 to 10) foreach (i => level.lower(i).assertGet shouldBe lowerPut)
                    (0 to 5) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  } else {
                    (0 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  }

                case (upperPut: Memory.Put, _) =>
                  if (upperPut.hasTimeLeft())
                    (11 to 22) foreach (i => level.lower(i).assertGet shouldBe upperPut)
                  else
                    (11 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                  (0 to 10) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                case (_: Memory.Update, put: Memory.Put) =>
                  if (put.hasTimeLeft())
                    (6 to 22) foreach (i => level.lower(i).assertGet shouldBe put)
                  else
                    (6 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                  (0 to 5) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                case (_: Memory.Update, _) =>
                  (0 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

              }
        )
      }
    }

    "Mid" in {
      //run this test multiple times to execute on multiple possible combinations
      //        10 - 20
      //    5
      runThis(maxIterations.times) {
        val upperLevelKeyValue = if (Random.nextBoolean()) randomRangeKeyValue(10, 20) else randomFixedKeyValue(10)
        val lowerLevelKeyValue = if (Random.nextBoolean()) randomRangeKeyValue(5, 20) else randomFixedKeyValue(5)

        //      val upperLevelKeyValue = Memory.Put(10, Some("put"), Some(0.seconds.fromNow))
        //      val lowerLevelKeyValue = Memory.Update(5, Some("update"), Some(10.seconds.fromNow))

        println
        println("upperLevelKeyValue: " + upperLevelKeyValue)
        println("lowerLevelKeyValue: " + lowerLevelKeyValue)

        assertOnLevel(
          upperLevelKeyValues = Slice(upperLevelKeyValue),
          lowerLevelKeyValues = Slice(lowerLevelKeyValue),
          assertion =
            level =>
              (toFixed(upperLevelKeyValue), toFixed(lowerLevelKeyValue)) match {
                //If remove hasTimeLeft and lower Level's range has Put which is also not expired, then lower should return the Put's value.
                case (_: Memory.Remove, put: Memory.Put) =>
                  //expected Put will contain Remove's deadline if deadline exists else put's deadline.
                  if (put.hasTimeLeft())
                    (6 to 22) foreach (i => level.lower(i).assertGet shouldBe put)
                  else
                    (6 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                  (0 to 5) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                case (_: Memory.Remove, _) =>
                  (0 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                case (upperPut: Memory.Put, lowerPut: Memory.Put) =>
                  if (upperPut.hasTimeLeft() && !lowerPut.hasTimeLeft()) {
                    (11 to 22) foreach (i => level.lower(i).assertGet shouldBe upperPut)
                    (0 to 10) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  } else if (!upperPut.hasTimeLeft() && lowerPut.hasTimeLeft()) {
                    (6 to 22) foreach (i => level.lower(i).assertGet shouldBe lowerPut)
                    (0 to 5) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  } else if (upperPut.hasTimeLeft() && lowerPut.hasTimeLeft()) {
                    (11 to 22) foreach (i => level.lower(i).assertGet shouldBe upperPut)
                    (6 to 10) foreach (i => level.lower(i).assertGet shouldBe lowerPut)
                    (0 to 5) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  } else {
                    (0 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  }

                case (upperPut: Memory.Put, _) =>
                  if (upperPut.hasTimeLeft())
                    (11 to 22) foreach (i => level.lower(i).assertGet shouldBe upperPut)
                  else
                    (11 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                  (0 to 10) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                case (_: Memory.Update, put: Memory.Put) =>
                  if (put.hasTimeLeft())
                    (6 to 22) foreach (i => level.lower(i).assertGet shouldBe put)
                  else
                    (6 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                  (0 to 5) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                case (_: Memory.Update, _) =>
                  (0 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
              }
        )
      }
    }

    "Right" in {
      //run this test multiple times to execute on multiple possible combinations
      //0
      //      5 - 10
      //        7 ----to----> ∞
      runThis(maxIterations.times) {

        val upperLevelKeyValue = if (Random.nextBoolean()) randomRangeKeyValue(5, 10) else randomFixedKeyValue(5)
        val lowerLevelKeyValue = if (Random.nextBoolean()) randomRangeKeyValue(7, 20) else randomFixedKeyValue(7)

        //      val upperLevelKeyValue = Memory.Range(5, 10, Some(Value.Put(None, 10000.seconds)), Value.Remove(20000.seconds.fromNow))
        //      val lowerLevelKeyValue = Memory.Range(7, 15, Some(Value.Put(7, 10000.seconds)), Value.Remove(None))

        //        val upperLevelKeyValue = Memory.Remove(5, randomDeadlineOption)
        //        val lowerLevelKeyValue = Memory.Range(7, 15, Some(Value.Put(Some(7), randomDeadlineOption)), Value.Remove(randomDeadlineOption))

        println
        println("upperLevelKeyValue: " + upperLevelKeyValue)
        println("lowerLevelKeyValue: " + lowerLevelKeyValue)

        //convert lower to fixed key-value by merging overlapping upper with lower.
        //convenience for writing test cases.
        val expectedLowerValue: ReadOnly.Fixed =
        (upperLevelKeyValue, lowerLevelKeyValue) match {
          case (upperRange: Memory.Range, lowerRange: Memory.Range) =>
            KeyValueMerger.applyValue(upperRange.rangeValue.toMemory(7), lowerRange.fromValue.getOrElse(lowerRange.rangeValue).toMemory(7), Duration.Zero).assertGet

          case (upperRange: Memory.Range, lower: Memory.Fixed) =>
            KeyValueMerger.applyValue(upperRange.rangeValue.toMemory(7), lower, Duration.Zero).assertGet

          case (_: Memory.Fixed, lowerRange: Memory.Range) =>
            toFixed(lowerRange)

          case (_: Memory.Fixed, lower: Memory.Fixed) =>
            lower
        }

        assertOnLevel(
          upperLevelKeyValues = Slice(upperLevelKeyValue),
          lowerLevelKeyValues = Slice(lowerLevelKeyValue),
          assertion =
            level =>
              (toFixed(upperLevelKeyValue), expectedLowerValue) match {
                //If remove hasTimeLeft and lower Level's range has Put which is also not expired, then lower should return the Put's value.
                case (_: Memory.Remove, put: Memory.Put) =>
                  //expected Put will contain Remove's deadline if deadline exists else put's deadline.
                  if (put.hasTimeLeft()) {
                    (8 to 22) foreach (i => level.lower(i).assertGet shouldBe put)
                    (0 to 7) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  } else {
                    (0 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  }

                case (_: Memory.Remove, _) =>
                  (0 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                case (upperPut: Memory.Put, lowerPut: Memory.Put) =>
                  if (upperPut.hasTimeLeft() && !lowerPut.hasTimeLeft()) {
                    (6 to 22) foreach (i => level.lower(i).assertGet shouldBe upperPut)
                    (0 to 5) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  } else if (!upperPut.hasTimeLeft() && lowerPut.hasTimeLeft()) {
                    (8 to 22) foreach (i => level.lower(i).assertGet shouldBe lowerPut)
                    (0 to 7) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  } else if (upperPut.hasTimeLeft() && lowerPut.hasTimeLeft()) {
                    (6 to 7) foreach (i => level.lower(i).assertGet shouldBe upperPut)
                    (8 to 22) foreach (i => level.lower(i).assertGet shouldBe lowerPut)
                    (0 to 5) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  } else {
                    (0 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  }

                case (upperPut: Memory.Put, _) =>
                  if (upperPut.hasTimeLeft())
                    (6 to 22) foreach (i => level.lower(i).assertGet shouldBe upperPut)
                  else
                    (6 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                  (0 to 5) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                case (_: Memory.Update, put: Memory.Put) =>
                  if (put.hasTimeLeft()) {
                    (8 to 22) foreach (i => level.lower(i).assertGet shouldBe put)
                    (0 to 7) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  } else
                    (0 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                case (_: Memory.Update, _) =>
                  (0 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
              }
        )
      }
    }

    "Right - 2" in {
      //run this test multiple times to execute on multiple possible combinations
      //0
      //      5 - 10
      //          10 ----to----> ∞
      runThis(maxIterations.times) {

        val upperLevelKeyValue = if (Random.nextBoolean()) randomRangeKeyValue(5, 10) else randomFixedKeyValue(5)
        val lowerLevelKeyValue = if (Random.nextBoolean()) randomRangeKeyValue(10, 20) else randomFixedKeyValue(10)

        //      val upperLevelKeyValue = Memory.Range(5, 10, Some(Value.Put(None, 10000.seconds)), Value.Remove(20000.seconds.fromNow))
        //      val lowerLevelKeyValue = Memory.Range(7, 15, Some(Value.Put(7, 10000.seconds)), Value.Remove(None))

        //        val upperLevelKeyValue = Memory.Remove(5, randomDeadlineOption)
        //        val lowerLevelKeyValue = Memory.Range(7, 15, Some(Value.Put(Some(7), randomDeadlineOption)), Value.Remove(randomDeadlineOption))

        println
        println("upperLevelKeyValue: " + upperLevelKeyValue)
        println("lowerLevelKeyValue: " + lowerLevelKeyValue)

        //convert lower to fixed key-value by merging overlapping upper with lower.
        //convenience for writing test cases.
        assertOnLevel(
          upperLevelKeyValues = Slice(upperLevelKeyValue),
          lowerLevelKeyValues = Slice(lowerLevelKeyValue),
          assertion =
            level =>
              (toFixed(upperLevelKeyValue), toFixed(lowerLevelKeyValue)) match {
                //If remove hasTimeLeft and lower Level's range has Put which is also not expired, then lower should return the Put's value.
                case (_: Memory.Remove, put: Memory.Put) =>
                  //expected Put will contain Remove's deadline if deadline exists else put's deadline.
                  if (put.hasTimeLeft()) {
                    (11 to 22) foreach (i => level.lower(i).assertGet shouldBe put)
                    (0 to 10) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  } else {
                    (0 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  }

                case (_: Memory.Remove, _) =>
                  (0 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                case (upperPut: Memory.Put, lowerPut: Memory.Put) =>
                  if (upperPut.hasTimeLeft() && !lowerPut.hasTimeLeft()) {
                    (6 to 22) foreach (i => level.lower(i).assertGet shouldBe upperPut)
                    (0 to 5) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  } else if (!upperPut.hasTimeLeft() && lowerPut.hasTimeLeft()) {
                    (11 to 22) foreach (i => level.lower(i).assertGet shouldBe lowerPut)
                    (0 to 10) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  } else if (upperPut.hasTimeLeft() && lowerPut.hasTimeLeft()) {
                    (6 to 10) foreach (i => level.lower(i).assertGet shouldBe upperPut)
                    (11 to 22) foreach (i => level.lower(i).assertGet shouldBe lowerPut)
                    (0 to 5) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  } else {
                    (0 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  }

                case (upperPut: Memory.Put, _) =>
                  if (upperPut.hasTimeLeft())
                    (6 to 22) foreach (i => level.lower(i).assertGet shouldBe upperPut)
                  else
                    (6 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                  (0 to 5) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                case (_: Memory.Update, put: Memory.Put) =>
                  if (put.hasTimeLeft()) {
                    (11 to 22) foreach (i => level.lower(i).assertGet shouldBe put)
                    (0 to 10) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
                  }
                  else
                    (0 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)

                case (_: Memory.Update, _) =>
                  (0 to 22) foreach (i => level.lower(i).assertGetOpt shouldBe empty)
              }
        )
      }
    }
  }
}