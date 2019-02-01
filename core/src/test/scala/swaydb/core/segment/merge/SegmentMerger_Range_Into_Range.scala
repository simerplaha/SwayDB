/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

package swaydb.core.segment.merge

import org.scalatest.WordSpec
import swaydb.core.{CommonAssertions, TestTimeGenerator}
import swaydb.core.data.{Memory, Value}
import swaydb.data.slice.Slice
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.serializers.Default._
import swaydb.serializers._
import scala.concurrent.duration._
import swaydb.core.merge.{ApplyMerger, FixedMerger, ValueMerger}
import swaydb.core.TestData._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.IOAssert._

class SegmentMerger_Range_Into_Range extends WordSpec {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def groupingStrategy = randomGroupingStrategyOption(randomNextInt(1000))

  implicit val timeGenerator = TestTimeGenerator.Empty

  "Range into Range" when {
    "1" in {
      //1   -     15
      //      10   -  20
      runThis(10000.times) {
        val newKeyValue = Memory.Range(1, 15, randomFromValue(), randomRangeValue())
        val oldKeyValue = Memory.Range(10, 20, randomFromValue(), randomRangeValue())

        val expectedKeyValue =
          Slice(
            Memory.Range(
              fromKey = 1,
              toKey = 10,
              fromValue = newKeyValue.fromValue,
              rangeValue = newKeyValue.rangeValue
            ),
            Memory.Range(
              fromKey = 10,
              toKey = 15,
              fromValue = oldKeyValue.fromValue.map(fromValue => FixedMerger(newKeyValue.rangeValue.toMemory(10), fromValue.toMemory(10)).flatMap(_.toFromValue()).assertGet),
              rangeValue = FixedMerger(newKeyValue.rangeValue.toMemory(Slice.emptyBytes), oldKeyValue.rangeValue.toMemory(Slice.emptyBytes)).flatMap(_.toRangeValue()).assertGet
            ),
            Memory.Range(
              fromKey = 15,
              toKey = 20,
              fromValue = None,
              rangeValue = oldKeyValue.rangeValue
            )
          )
        val expectedLastLevel = expectedKeyValue.flatMap(keyValue => keyValue.fromValue.flatMap(_.toExpectedLastLevelKeyValue(keyValue.key))).toSlice

        //        println
        //        println("newKeyValue: " + newKeyValue)
        //        println("oldKeyValue: " + oldKeyValue)
        //        println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
        //        println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = expectedLastLevel
        )
      }
    }

    "2" in {
      //1      -      20
      //      10   -  20
      runThis(10000.times) {
        val newKeyValue = Memory.Range(1, 20, randomFromValueOption(), randomRangeValue())
        val oldKeyValue = Memory.Range(10, 20, randomFromValueOption(), randomRangeValue())

        val expectedKeyValue =
          Slice(
            Memory.Range(
              fromKey = 1,
              toKey = 10,
              fromValue = newKeyValue.fromValue,
              rangeValue = newKeyValue.rangeValue
            ),
            Memory.Range(
              fromKey = 10,
              toKey = 20,
              fromValue = oldKeyValue.fromValue.map(oldFromValue => FixedMerger(newKeyValue.rangeValue.toMemory(10), oldFromValue.toMemory(10)).flatMap(_.toFromValue()).assertGet),
              rangeValue = FixedMerger(newKeyValue.rangeValue.toMemory(Slice.emptyBytes), oldKeyValue.rangeValue.toMemory(Slice.emptyBytes)).flatMap(_.toRangeValue()).assertGet
            )
          )
        val expectedLastLevel = expectedKeyValue.flatMap(keyValue => keyValue.fromValue.flatMap(_.toExpectedLastLevelKeyValue(keyValue.key))).toSlice

        //println
        //println("newKeyValue: " + newKeyValue)
        //println("oldKeyValue: " + oldKeyValue)
        //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
        //println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = expectedLastLevel
        )
      }
    }

    "3" in {
      //1      -         21
      //      10   -  20
      runThis(10000.times) {
        val newKeyValue = Memory.Range(1, 21, randomFromValueOption(), randomRangeValue())
        val oldKeyValue = Memory.Range(10, 20, randomFromValueOption(), randomRangeValue())

        val expectedKeyValue =
          Slice(
            Memory.Range(
              fromKey = 1,
              toKey = 10,
              fromValue = newKeyValue.fromValue,
              rangeValue = newKeyValue.rangeValue
            ),
            Memory.Range(
              fromKey = 10,
              toKey = 20,
              fromValue = oldKeyValue.fromValue.map(oldFromValue => FixedMerger(newKeyValue.rangeValue.toMemory(10), oldFromValue.toMemory(10)).flatMap(_.toFromValue()).assertGet),
              rangeValue = FixedMerger(newKeyValue.rangeValue.toMemory(Slice.emptyBytes), oldKeyValue.rangeValue.toMemory(Slice.emptyBytes)).flatMap(_.toRangeValue()).assertGet
            ),
            Memory.Range(
              fromKey = 20,
              toKey = 21,
              fromValue = None,
              rangeValue = newKeyValue.rangeValue
            )
          )
        val expectedLastLevel = expectedKeyValue.flatMap(keyValue => keyValue.fromValue.flatMap(_.toExpectedLastLevelKeyValue(keyValue.key))).toSlice

        //println
        //println("newKeyValue: " + newKeyValue)
        //println("oldKeyValue: " + oldKeyValue)
        //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
        //println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = expectedLastLevel
        )
      }
    }

    "4" in {
      //      10 - 15
      //      10   -  20
      runThis(10000.times) {
        val newKeyValue = Memory.Range(10, 15, randomFromValueOption(), randomRangeValue())
        val oldKeyValue = Memory.Range(10, 20, randomFromValueOption(), randomRangeValue())

        //different approach used pattern matching instead of using similar approach just like in SegmentMerge.scala
        val from: Option[Value.FromValue] =
          oldKeyValue.fromValue match {
            case Some(oldFromValue) =>
              Some(FixedMerger(newKeyValue.fromValue.getOrElse(newKeyValue.rangeValue).toMemory(10), oldFromValue.toMemory(10)).flatMap(_.toFromValue()).assertGet)
            case None =>
              newKeyValue.fromValue match {
                case Some(newFromValue) =>
                  Some(FixedMerger(newFromValue.toMemory(10), oldKeyValue.fromValue.getOrElse(oldKeyValue.rangeValue).toMemory(10)).flatMap(_.toFromValue()).assertGet)
                case None =>
                  None
              }
          }

        val expectedKeyValue =
          Slice(
            Memory.Range(
              fromKey = 10,
              toKey = 15,
              fromValue = from,
              rangeValue = ValueMerger(newKeyValue.rangeValue: Value.RangeValue, oldKeyValue.rangeValue: Value.RangeValue).assertGet
            ),
            Memory.Range(
              fromKey = 15,
              toKey = 20,
              fromValue = None,
              rangeValue = oldKeyValue.rangeValue
            )
          )
        val expectedLastLevel = expectedKeyValue.flatMap(keyValue => keyValue.fromValue.flatMap(_.toExpectedLastLevelKeyValue(keyValue.key))).toSlice

        //println
        //println("newKeyValue: " + newKeyValue)
        //println("oldKeyValue: " + oldKeyValue)
        //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
        //println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = expectedLastLevel
        )
      }
    }

    "5" in {
      //      10   -  20
      //      10   -  20
      runThis(10000.times) {
        val newKeyValue = Memory.Range(10, 20, randomFromValueOption(), randomRangeValue())
        val oldKeyValue = Memory.Range(10, 20, randomFromValueOption(), randomRangeValue())

        val expectedKeyValue =
          Slice(
            Memory.Range(
              fromKey = 10,
              toKey = 20,
              fromValue =
                //different approach used pattern matching instead of using similar approach like in SegmentMerge.scala
                oldKeyValue.fromValue match {
                  case Some(oldFromValue) =>
                    Some(FixedMerger(newKeyValue.fromValue.getOrElse(newKeyValue.rangeValue).toMemory(10), oldFromValue.toMemory(10)).flatMap(_.toFromValue()).assertGet)
                  case None =>
                    newKeyValue.fromValue match {
                      case Some(newFromValue) =>
                        Some(FixedMerger(newFromValue.toMemory(10), oldKeyValue.fromValue.getOrElse(oldKeyValue.rangeValue).toMemory(10)).flatMap(_.toFromValue()).assertGet)
                      case None =>
                        None
                    }
                },
              rangeValue = FixedMerger(newKeyValue.rangeValue.toMemory(Slice.emptyBytes), oldKeyValue.rangeValue.toMemory(Slice.emptyBytes)).flatMap(_.toRangeValue()).assertGet
            )
          )
        val expectedLastLevel = expectedKeyValue.flatMap(keyValue => keyValue.fromValue.flatMap(_.toExpectedLastLevelKeyValue(keyValue.key))).toSlice

        //println
        //println("newKeyValue: " + newKeyValue)
        //println("oldKeyValue: " + oldKeyValue)
        //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
        //println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = expectedLastLevel
        )
      }
    }

    "6" in {
      //      10   -     21
      //      10   -  20
      runThis(10000.times) {
        val newKeyValue = Memory.Range(10, 21, randomFromValueOption(), randomRangeValue())
        val oldKeyValue = Memory.Range(10, 20, randomFromValueOption(), randomRangeValue())

        val expectedKeyValue =
          Slice(
            Memory.Range(
              fromKey = 10,
              toKey = 20,
              fromValue =
                //different approach used pattern matching instead of using similar approach just like in SegmentMerge.scala
                oldKeyValue.fromValue match {
                  case Some(oldFromValue) =>
                    Some(FixedMerger(newKeyValue.fromValue.getOrElse(newKeyValue.rangeValue).toMemory(10), oldFromValue.toMemory(10)).flatMap(_.toFromValue()).assertGet)
                  case None =>
                    newKeyValue.fromValue match {
                      case Some(newFromValue) =>
                        Some(FixedMerger(newFromValue.toMemory(10), oldKeyValue.fromValue.getOrElse(oldKeyValue.rangeValue).toMemory(10)).flatMap(_.toFromValue()).assertGet)
                      case None =>
                        None
                    }
                },
              rangeValue = FixedMerger(newKeyValue.rangeValue.toMemory(Slice.emptyBytes), oldKeyValue.rangeValue.toMemory(Slice.emptyBytes)).flatMap(_.toRangeValue()).assertGet
            ),
            Memory.Range(
              fromKey = 20,
              toKey = 21,
              fromValue = None,
              rangeValue = newKeyValue.rangeValue
            )
          )
        val expectedLastLevel = expectedKeyValue.flatMap(keyValue => keyValue.fromValue.flatMap(_.toExpectedLastLevelKeyValue(keyValue.key))).toSlice

        //        println
        //        println("newKeyValue: " + newKeyValue)
        //        println("oldKeyValue: " + oldKeyValue)
        //        println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
        //        println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = expectedLastLevel
        )
      }
    }

    "7" in {
      //        11 - 15
      //      10   -   20
      runThis(10000.times) {
        val newKeyValue = Memory.Range(11, 15, randomFromValueOption(), randomRangeValue())
        val oldKeyValue = Memory.Range(10, 20, randomFromValueOption(), randomRangeValue())

        val expectedKeyValue =
          Slice(
            Memory.Range(
              fromKey = 10,
              toKey = 11,
              fromValue = oldKeyValue.fromValue,
              rangeValue = oldKeyValue.rangeValue
            ),
            Memory.Range(
              fromKey = 11,
              toKey = 15,
              fromValue = newKeyValue.fromValue.map(fromValue => FixedMerger(fromValue.toMemory(11), oldKeyValue.rangeValue.toMemory(11)).flatMap(_.toFromValue()).assertGet),
              rangeValue = FixedMerger(newKeyValue.rangeValue.toMemory(Slice.emptyBytes), oldKeyValue.rangeValue.toMemory(Slice.emptyBytes)).flatMap(_.toRangeValue()).assertGet
            ),
            Memory.Range(
              fromKey = 15,
              toKey = 20,
              fromValue = None,
              rangeValue = oldKeyValue.rangeValue
            )
          )
        val expectedLastLevel = expectedKeyValue.flatMap(keyValue => keyValue.fromValue.flatMap(_.toExpectedLastLevelKeyValue(keyValue.key))).toSlice

        //println
        //println("newKeyValue: " + newKeyValue)
        //println("oldKeyValue: " + oldKeyValue)
        //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
        //println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = expectedLastLevel
        )
      }
    }

    "8" in {
      //        11 -   20
      //      10   -   20
      runThis(1000.times) {
        val newKeyValue = Memory.Range(11, 20, randomFromValueOption(), randomRangeValue())
        val oldKeyValue = Memory.Range(10, 20, randomFromValueOption(), randomRangeValue())

        val expectedKeyValue =
          Slice(
            Memory.Range(
              fromKey = 10,
              toKey = 11,
              fromValue = oldKeyValue.fromValue,
              rangeValue = oldKeyValue.rangeValue
            ),
            Memory.Range(
              fromKey = 11,
              toKey = 20,
              fromValue = newKeyValue.fromValue.map(fromValue => FixedMerger(fromValue.toMemory(11), oldKeyValue.rangeValue.toMemory(11)).flatMap(_.toFromValue()).assertGet),
              rangeValue = FixedMerger(newKeyValue.rangeValue.toMemory(Slice.emptyBytes), oldKeyValue.rangeValue.toMemory(Slice.emptyBytes)).flatMap(_.toRangeValue()).assertGet
            )
          )
        val expectedLastLevel = expectedKeyValue.flatMap(keyValue => keyValue.fromValue.flatMap(_.toExpectedLastLevelKeyValue(keyValue.key))).toSlice

        //println
        //println("newKeyValue: " + newKeyValue)
        //println("oldKeyValue: " + oldKeyValue)
        //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
        //println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = expectedLastLevel
        )
      }
    }

    "9" in {
      //        11 -     21
      //      10   -   20
      runThis(1000.times) {
        val newKeyValue = Memory.Range(11, 21, randomFromValueOption(), randomRangeValue())
        val oldKeyValue = Memory.Range(10, 20, randomFromValueOption(), randomRangeValue())

        val expectedKeyValue =
          Slice(
            Memory.Range(
              fromKey = 10,
              toKey = 11,
              fromValue = oldKeyValue.fromValue,
              rangeValue = oldKeyValue.rangeValue
            ),
            Memory.Range(
              fromKey = 11,
              toKey = 20,
              fromValue = newKeyValue.fromValue.map(fromValue => FixedMerger(fromValue.toMemory(11), oldKeyValue.rangeValue.toMemory(11)).flatMap(_.toFromValue()).assertGet),
              rangeValue = FixedMerger(newKeyValue.rangeValue.toMemory(Slice.emptyBytes), oldKeyValue.rangeValue.toMemory(Slice.emptyBytes)).flatMap(_.toRangeValue()).assertGet
            ),
            Memory.Range(
              fromKey = 20,
              toKey = 21,
              fromValue = None,
              rangeValue = newKeyValue.rangeValue
            )
          )
        val expectedLastLevel = expectedKeyValue.flatMap(keyValue => keyValue.fromValue.flatMap(_.toExpectedLastLevelKeyValue(keyValue.key))).toSlice

        //println
        //println("newKeyValue: " + newKeyValue)
        //println("oldKeyValue: " + oldKeyValue)
        //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
        //println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = expectedLastLevel
        )
      }
    }
  }
}
